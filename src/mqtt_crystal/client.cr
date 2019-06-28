require "uuid"
require "uri"
require "socket"
require "openssl"

class MqttCrystal::Client
  private abstract class SocketLike
    abstract def connect(host : String, port : UInt16)
    abstract def read(slice : Bytes)
    abstract def write(slice : Bytes)
    abstract def close
  end

  property id, host, port, username, password

  @socket : SocketLike
  @topics = Array(String).new
  @channel = Channel(Packet).new
  @next_packet_id = 0_u16
  @connecting = false
  @connected = false
  @subscribed = false
  @stop = false
  @buffer = Array(UInt8).new

  @id : String
  @random_id = true

  def initialize(@host : String = "127.0.0.1",
                 @port : UInt16 = 1883_u16,
                 @username : String | Nil = nil,
                 @password : String | Nil = nil,
                 url : String | Nil = nil,
                 id : String | Nil = nil,
                 keep_alive : UInt16 = 15_u16,
                 @auto_reconnect : Bool = true,
                 @tls : Bool = false)
    if url
      uri = URI.parse url.not_nil!
      @host = uri.host.not_nil! if uri.host
      @port = uri.port.not_nil!.to_u16 if uri.port
      @username = uri.user
      @password = uri.password
    end

    @socket = @tls ? TlsSocket.new : RegularSocket.new
    @random_id = id.nil?
    @id = id || "s-#{UUID.random.to_s}"

    spawn do
      while !@stop
        sleep keep_alive.seconds
        send Packet::Pingreq.new
      end
    end
  end

  def listen(&block)
    while !@stop
      packet = @channel.receive
      if packet.is_a?(Packet::Publish)
        packet = packet.as(Packet::Publish)
        yield(packet.topic, packet.payload)
        send Packet::Puback.new(id: packet.id) if packet.qos > 0
      end
    end
  end

  def get(topic : String)
    subscribe topic
    listen { |topic, payload| yield topic, payload }
  rescue e
    return self if @stop
    puts "get failed #{e}"
  end

  def subscribe(topic : String) : self
    subscribe([topic])
  end

  def subscribe(topics : Array(String)) : self
    connect unless @connected
    @topics += topics
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
          @channel.send packet
        end
      end
    rescue spawn_read_e
      pp spawn_read_e
      reconnect
    end

    @socket.write Packet::Connect.new(client_id: @id, username: @username, password: @password).bytes

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
    begin
      @socket.close
    rescue e
    end
    sleep 1
    @id = "s-#{UUID.random.to_s}" if @random_id
    @socket = @tls ? TlsSocket.new : RegularSocket.new
    connect
    subscribe @topics

    self
  end

  def publish(topic : String, payload : String, qos : UInt8 = 1_u8, retain : Bool = false)
    connect unless @connected
    send Packet::Publish.new(id: next_packet_id, qos: qos, retain: retain, topic: topic, payload: payload)
  end

  private def send(packet : Packet)
    return if @stop
    while !@stop && !@connected
      sleep 0.5
    end
    @socket.write packet.bytes
  end

  private def next_packet_id
    @next_packet_id += 1
  end

  def connected?
    @connected
  end

  def subscribed?
    @subscribed
  end

  def close
    @stop = true
    @connected = false
    @channel.close
    @socket.close
  end

  DEFAULT_SOCKET_ARGS = {
    family:   Socket::Family::INET,
    type:     Socket::Type::STREAM,
    protocol: Socket::Protocol::TCP,
    blocking: false,
  }

  private class RegularSocket < SocketLike
    def initialize
      @socket = Socket.new(**DEFAULT_SOCKET_ARGS)
    end

    def connect(host : String, port : UInt16)
      @socket.connect(host, port)
    end

    def read(*args)
      @socket.read *args
    end

    def write(*args)
      @socket.write *args
    end

    def close
      @socket.close
    end
  end

  private class TlsSocket < SocketLike
    @tls_socket : OpenSSL::SSL::Socket::Client | Nil = nil

    def initialize
      @socket = Socket.new(**DEFAULT_SOCKET_ARGS)
    end

    def connect(host : String, port : UInt16)
      @socket.connect(host, port)
      ctx = OpenSSL::SSL::Context::Client.new
      @tls_socket = OpenSSL::SSL::Socket::Client.new(@socket, ctx)
    end

    def read(*args)
      @tls_socket.not_nil!.unbuffered_read *args
    end

    def write(*args)
      @tls_socket.not_nil!.unbuffered_write *args
    end

    def close
      @tls_socket.not_nil!.close
    end
  end
end
