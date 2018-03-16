require "uuid"
require "socket"

class MqttCrystal::Client
  property socket, id, next_packet_id, stop

  def initialize(@socket : IO = IO::Memory.new,
                 @id : String = "S-#{UUID.random.to_s}",
                 @next_packet_id : UInt16 = 0_u16,
                 @stop : Bool = false); end

  def connect(host : String = "127.0.0.1",
              port : UInt16 = 1883_u16,
              username : String | Nil = nil,
              password : String | Nil = nil)
    @socket = Socket.new(family: Socket::Family::INET,
                         type: Socket::Type::STREAM,
                         protocol: Socket::Protocol::TCP,
                         blocking: true)
    @socket.as(Socket).connect(host: host, port: port)
    send MqttCrystal::Packet::Connect.new(client_id: @id, username: username, password: password)
    read_packet
  end

  def subscribe(topic : String = "pub/#")
    send MqttCrystal::Packet::Subscribe.new(next_packet_id, topic)
    read_packet
  end

  def get
    while !stop
      topic = payload = nil
      begin
        raise "socket not connected" if !connected?
        packet = read_packet
        if packet.is_a?(Packet::Publish)
          topic = packet.topic
          payload = packet.payload
        end
      rescue e
        puts "handle packet failed: #{e}, reconnect"
        sleep 1.second
        connect if !stop
      end

      yield topic, payload if topic && payload
    end
  end

  def publish(topic : String, message : String)
    send MqttCrystal::Packet::Publish.new(topic: topic, payload: message)
  end

  def ping
    send MqttCrystal::Packet::Pingreq.new
    read_packet
  end

  def keep_alive
    while !stop
      ping
      sleep 15.seconds
    end
  end

  def connected?; !@socket.nil? && !@socket.not_nil!.closed? end

  def send(packet : MqttCrystal::Packet)
    return if stop
    raise "socket not connected" if !connected?
    slice = packet.bytes
    @socket.not_nil!.write slice
  rescue e
    puts "send failed: #{e}, reconnect"
    sleep 1.second
    connect
  end

  def next_packet_id; @next_packet_id += 1 end

  def read_packet : Packet
    packet = nil
    while !stop
      packet = Packet.create_from_header(socket.read_byte)
      break if packet && packet.validate_flags
    end
    packet = packet || Packet::Pingresp.new

    multiplier = 1
    body_length = 0_u64
    pos = 1

    while !stop
      digit = socket.read_byte
      next if digit.nil?
      body_length += ((digit & 0x7F) * multiplier)
      multiplier *= 0x80
      pos += 1
      break if (digit & 0x80).zero? || pos > 4
    end

    packet.not_nil!.body_length = body_length
    slice = Bytes.new(body_length)
    socket.read slice
    packet.not_nil!.parse_body(slice)

    if (packet.not_nil!.is_a?(Packet::Publish))
      packet = packet.not_nil!.as(Packet::Publish)
      if packet.qos > 0
        send MqttCrystal::Packet::Puback.new(id: packet.id)
      end
    end

    packet.not_nil!
  end

  def close
    stop = true
  end
end
