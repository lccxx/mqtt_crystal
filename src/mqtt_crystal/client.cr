require "uuid"
require "uri"
require "socket"

class MqttCrystal::Client
  DEFAULT_SOCKET_ARGS = {
    family: Socket::Family::INET,
    type: Socket::Type::STREAM,
    protocol: Socket::Protocol::TCP,
    blocking: false
  }

  property id, host, port, username, password, keep_alive, socket, channel, next_packet_id

  def initialize(@id : String = "s-#{UUID.random.to_s}",
                 @host : String = "127.0.0.1",
                 @port : UInt16 = 1883_u16,
                 @username : String | Nil = nil,
                 @password : String | Nil = nil,
                 @url : String | Nil = nil,
                 @keep_alive : UInt16 = 15_u16,
                 @auto_reconnect : Bool = true,
                 @topics : Array(String) = Array(String).new,
                 @socket : Socket = Socket.new(**DEFAULT_SOCKET_ARGS),
                 @channel : Channel(Packet) = Channel(Packet).new,
                 @next_packet_id : UInt16 = 0_u16,
                 @connecting : Bool = false,
                 @connected : Bool = false,
                 @subscribed : Bool = false,
                 @stop : Bool = false,
                 @buffer : Array(UInt8) = Array(UInt8).new)
    if @url
      uri = URI.parse @url.not_nil!
      @host = uri.host.not_nil! if uri.host
      @port = uri.port.not_nil!.to_u16 if uri.port
      @username = uri.user
      @password = uri.password
    end

    spawn do
      while !@stop
        sleep @keep_alive.seconds
        send Packet::Pingreq.new
      end
    end
  end

  def get(topic : String)
    subscribe topic
    while !@stop
      packet = channel.receive
      if packet.is_a?(Packet::Publish)
        packet = packet.as(Packet::Publish)
        yield(packet.topic, packet.payload)
        send Packet::Puback.new(id: packet.id) if packet.qos > 0
      end
    end
  rescue e
    return self if @stop
    puts "get failed #{e}"
  end

  def subscribe(topic : String) : self
    connect if !@connected

    @topics << topic
    subscribe([ topic ])
  end

  def subscribe(topics : Array(String)) : self
    topics.each { |topic| send Packet::Subscribe.new(id: next_packet_id, topic: topic) }

    self
  end

  def connect : self
    return self if @connecting || @connected || @stop
    @connecting = true

    @socket.connect(host: @host, port: @port)

    slice = Bytes.new(1 << 10 * 2)
    spawn do
      while !@stop
        count = @socket.read slice
        raise "read failed" if count == 0
        count.times { |i| @buffer << slice[i] }

        while packet = Packet.parse(@buffer)
          if packet.is_a?(Packet::Connack)
            @connecting = false
            @connected = true
          elsif packet.is_a?(Packet::Suback)
            @subscribed = true
          end
          channel.send packet
        end
      end
    rescue spawn_read_e
      pp spawn_read_e
      reconnect
    end 

    socket.write Packet::Connect.new(client_id: @id, username: @username, password: @password).bytes 

    self
  rescue connect_e
    pp connect_e
    reconnect
  end

  def connect
    connect

    begin
      yield self
    ensure
      close
    end
  end

  def reconnect : self
    return self if !@auto_reconnect
    @connecting = @connected = @subscribed = false
    begin; @socket.close; rescue e; end
    sleep 1
    @id = "s-#{UUID.random.to_s}"
    @socket = Socket.new(**DEFAULT_SOCKET_ARGS)
    connect
    subscribe @topics

    self
  end

  def publish(topic : String, payload : String)
    send Packet::Publish.new(id: next_packet_id, qos: 1_u8, topic: topic, payload: payload)
  end

  def send(packet : Packet)
    return if @stop
    while !@stop && !@connected; sleep 0.5 end
    socket.write packet.bytes
  end

  def next_packet_id; @next_packet_id += 1 end

  def connected?; @connected end

  def subscribed?; @subscribed end

  def close
    @stop = true
    @connected = false
    @channel.close
    @socket.close
  end
end
