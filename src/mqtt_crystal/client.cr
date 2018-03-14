require "uuid"
require "socket"

class MqttCrystal::Client
  property socket, id, next_packet_id, stop

  def initialize(@socket : IO,
                 @id : String = "S-#{UUID.random.to_s}",
                 @next_packet_id : UInt16 = 0_u16,
                 @stop : Bool = false); end

  def connect(username : String | Nil = nil, password : String | Nil = nil)
    send MqttCrystal::Packet::Connect.new(client_id: @id, username: username, password: password)
    read_packet
  end

  def subscribe(topic : String)
    send MqttCrystal::Packet::Subscribe.new(next_packet_id, topic)
    read_packet
  end

  def connected?; !@socket.nil? && !@socket.not_nil!.closed? end

  def get(topic : String = "pub/#")
    send MqttCrystal::Packet::Subscribe.new(next_packet_id, topic)

    while !stop
      begin
        raise "socket not connected" if !connected?
        packet = MqttCrystal::Packet.read(@socket.not_nil!)
        next sleep 0.01 if packet.nil?
        packet
      rescue e
        puts "handle packet failed: #{e}, reconnect"
        sleep 1.second
        connect
      end

      yield topic, "m" if false
    end
  end

  def publish(topic : String, message : String)
    send MqttCrystal::Packet::Publish.new(topic: topic, payload: message)
  end

  def keep_alive
    while !stop
      send MqttCrystal::Packet::Pingreq.new
      sleep 15.seconds
    end
  end

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

  def read_byte : UInt8
    b = nil
    while !stop
      b = socket.read_byte
      break if b
      sleep 0.01
    end
    b || 0xd0_u8
  end

  def read_packet : Packet
    packet = nil
    while !stop
      packet = Packet.create_from_header(read_byte)
      break if packet
      sleep 0.01
    end
    packet = packet || Packet::Pingresp.new

    multiplier = 1
    body_length = 0_u64
    pos = 1

    while !stop
      digit = read_byte
      body_length += ((digit & 0x7F) * multiplier)
      multiplier *= 0x80
      pos += 1
      break if (digit & 0x80).zero? || pos > 4
    end

    packet.not_nil!.body_length = body_length
    slice = Bytes.new(body_length)
    socket.read slice
    packet.not_nil!.parse_body(slice)

    packet.not_nil!
  end

  def close
    stop = true
  end
end
