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
      end
    end
  rescue e
    return self if @stop
    puts "get failed #{e}"
  end

  def subscribe(topic) : self
    connect if !@connected

    socket.write MqttCrystal::Packet::Subscribe.new(topic: topic).bytes

    self
  end

  def connect : self
    return self if @connected || @stop
    @connected = true
    @socket.connect(host: @host, port: @port)

    slice = Bytes.new(1 << 10 * 2)
    spawn do
      begin
        while !@stop && @connected
          count = @socket.read slice
          raise "read failed" if count == 0
          bytes = Array(UInt8).new(count)
          count.times { |i| bytes << slice[i] }
          channel.send Packet.parse(bytes)
        end
      rescue e
        @connected = false
        begin; @socket.close; rescue e; end
        puts "connect error: #{e}"
        if @auto_reconnect
          sleep 1
          connect
        end
        self
      end
    end

    spawn do
      while !@stop && @connected
        sleep @keep_alive.seconds
        socket.write MqttCrystal::Packet::Pingreq.new.bytes
      end
    end

    socket.write MqttCrystal::Packet::Connect.new(client_id: @id,
                                                  username: @username,
                                                  password: @password).bytes


    self
  end

  def connect
    begin
      yield self
    ensure
      close
    end
  end

  def publish(topic : String, payload : String)
    return if @stop
    connect if !@connected
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
