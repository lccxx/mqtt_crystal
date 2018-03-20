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

  def initialize(@id : String = "S-#{UUID.random.to_s}",
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
                 @connected : Bool = false,
                 @stop : Bool = false)
    if @url
      uri = URI.parse @url.not_nil!
      @host = uri.host.not_nil! if uri.host
      @port = uri.port.not_nil!.to_u16 if uri.port
      @username = uri.user
      @password = uri.password
    end
  end

  def get(topic : String)
    subscribe topic
    while !@stop
      packet = channel.receive
      if packet.is_a?(Packet::Publish)
        packet = packet.as(Packet::Publish)
        yield(packet.topic, packet.payload)
        socket.write Packet::Puback.new(id: packet.id).bytes if packet.qos > 0
      end
    end
  rescue e
    return self if @stop
    puts "get failed #{e}"
  end

  def subscribe(topic : String) : self
    connect if !@connected

    socket.write MqttCrystal::Packet::Subscribe.new(topic: topic).bytes
    @topics << topic

    self
  end

  def subscribe(topics : Array(String)) : self
    topics.each { |topic| subscribe(topic) }

    self
  end

  def connect : self
    return self if @connected || @stop
    @connected = true

    @socket.connect(host: @host, port: @port)

    @socket.write Packet::Connect.new(client_id: @id, username: @username, password: @password).bytes

    slice = Bytes.new(1 << 10 * 2)
    spawn do
      while !@stop && @connected
        count = @socket.read slice
        raise "read failed" if count == 0
        bytes = Array(UInt8).new(count)
        count.times { |i| bytes << slice[i] }
        # pp bytes.map { |b| b.chr }.join
        channel.send Packet.parse(bytes)
      end
    rescue spawn_read_e
      pp spawn_read_e
      reconnect
    end

    spawn do
      while !@stop && @connected
        sleep @keep_alive.seconds
        socket.write MqttCrystal::Packet::Pingreq.new.bytes
      end
    end

    self
  rescue connect_e
    pp connect_e
    reconnect
  end

  def connect
    begin
      yield self
    ensure
      close
    end
  end

  def reconnect : self
    return self if !@auto_reconnect
    @connected = false
    begin; @socket.close; rescue e; end
    sleep 1
    @socket = Socket.new(**DEFAULT_SOCKET_ARGS)
    connect
    subscribe @topics

    self
  end

  def publish(topic : String, payload : String)
    return if @stop
    while !@connected; sleep 0.5 end
    socket.write MqttCrystal::Packet::Publish.new(id: next_packet_id,
                                                  qos: 1_u8,
                                                  topic: topic,
                                                  payload: payload).bytes
  end

  def next_packet_id; @next_packet_id += 1 end

  def connected?; @connected end

  def close
    @stop = true
    @connected = false
    @channel.close
    @socket.close
  end
end
