class MqttCrystal::Packet
  property flags, body_length

  def self.parse(bytes : Array(UInt8)) : Packet
    packet = create_from_header(bytes.shift)
    return Pingresp.new if packet.nil? || !packet.validate_flags

    multiplier = 1
    body_length = 0_u64
    pos = 1

    while bytes.size > 0
      digit = bytes.shift
      body_length += ((digit & 0x7F) * multiplier)
      multiplier *= 0x80
      pos += 1
      break if (digit & 0x80).zero? || pos > 4
    end

    packet.body_length = body_length
    if bytes.size >= body_length
      packet.parse_body(bytes.shift(body_length))
    else
      pp [ "packet.parce error", packet, bytes.size, bytes ]
      return Pingresp.new
    end

    packet
  end

  def self.create_from_header(byte : UInt8 | Nil)
    return if byte.nil?
    type_id = ((byte & 0xF0) >> 4)
    packet_class = Packet::PACKET_TYPES[type_id]
    return if packet_class.nil?

    flags = Array(Bool).new()
    4.times { |i| flags << (byte & (1_u8 << i) != 0_u8) }

    packet_class.new(flags: flags)
  end

  def initialize(@flags : Array(Bool) = [ false, false, false, false ],
                 @body_length : UInt64 = 0_u64); end

  def validate_flags; true end

  def type_id : Int32
    PACKET_TYPES.index(self.class).not_nil!
  end

  def parse_body(buffer : Array(UInt8))

  end

  def encode_body : Bytes
    Bytes.new(0)
  end

  def slice_it(a : Array(UInt8)) : Bytes
    self.class.slice_it a
  end

  def self.slice_it(a : String) : Bytes
    slice_it a.bytes
  end

  def self.slice_it(a : Array(UInt8)) : Bytes
    slice = Bytes.new a.size
    a.map_with_index { |b ,i| slice[i] = b }
    slice
  end

  def concatenate(*args) : Bytes
    size = 0
    args.map { |a| size += a.size }
    slice = Bytes.new(size)
    index = 0
    args.map { |a| a.map { |b| slice[index] = b; index += 1 } }
    slice
  end

  def encode_short(n : UInt16) : Bytes
    slice = Bytes.new(2)
    slice[0] = (n >> 8).to_u8
    slice[1] = n.to_u8
    slice
  end

  def encode_string(str : String) : Bytes
    bytes = str.bytes
    size = bytes.size + 2
    slice = Bytes.new(size)
    size_slice = encode_short(bytes.size.to_u16)
    slice[0] = size_slice[0]
    slice[1] = size_slice[1]
    bytes.map_with_index { |b, i| slice[i + 2] = b }
    slice
  end

  def bytes : Bytes
    header = [
      ((type_id & 0x0F) << 4).to_u8 |
        (flags[3] ? 0x8_u8 : 0x0_u8) |
        (flags[2] ? 0x4_u8 : 0x0_u8) |
        (flags[1] ? 0x2_u8 : 0x0_u8) |
        (flags[0] ? 0x1_u8 : 0x0_u8)
      ]
    body = encode_body

    body_length = body.size

    while true
      digit = body_length % 128
      body_length = body_length / 128
      digit |= 0x80 if body_length > 0
      header.push(digit.to_u8)
      break if body_length <= 0
    end

    concatenate(header, body)
  end

  class Publish < Packet
    property id, topic, payload, qos

    def initialize(@topic : String = "pub/test",
                   @payload : String = "test",
                   @id : UInt16 = 0_u16,
                   @qos : UInt8 = 0_u8,
                   @flags : Array(Bool) = [ false, false, false, false ],
                   @body_length : UInt64 = 0_u64)
      if @qos != 0
        @flags[1] = (@qos & 0x01 == 0x01)
        @flags[2] = (@qos & 0x02 == 0x02)
      end
      if @flags != [ false, false, false, false ]
        @qos = (@flags[1] ? 0x01_u8 : 0x00_u8) | (@flags[2] ? 0x02_u8 : 0x00_u8)
      end
    end

    def encode_body
      concatenate(encode_string(@topic), @qos > 0 ? encode_short(@id) : Bytes.new(0), @payload.bytes)
    end

    def parse_body(buffer : Array(UInt8))
      return if buffer.size < 7
      len = (buffer[0] << 8) + buffer[1]
      len = buffer.size - 2 if qos.zero? && len > buffer.size - 2
      len = buffer.size - 4 if !qos.zero? && len > buffer.size - 4
      @topic = String.new(buffer[2, len])
      payload_start = len + 2
      payload_size = buffer.size - (len + 2)
      unless qos.zero?
        @id = (buffer[payload_start].to_u16 << 8) + buffer[payload_start + 1]
        payload_start += 2
        payload_size -= 2
      end
      @payload = String.new(buffer[payload_start, payload_size])
    end
  end

  class Connect < Packet
    def initialize(@client_id : String = UUID.random.to_s,
                   @username : String | Nil = nil,
                   @password : String | Nil = nil,
                   @protocol_name : String = "MQTT",
                   @protocol_level : UInt8 = 0x04_u8,
                   @keep_alive : UInt16 = 15_u16,
                   @flags : Array(Bool) = [ false, false, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def encode_body : Bytes
      clean_session = true
      will_topic = nil
      will_qos = 0_u8
      will_retain = false
      cflags = 0_u8
      cflags |= 0x02_u8 if clean_session
      cflags |= 0x04_u8 unless will_topic.nil?
      cflags |= ((will_qos & 0x03) << 3).to_u8
      cflags |= 0x20_u8 if will_retain
      cflags |= 0x40_u8 unless @password.nil?
      cflags |= 0x80_u8 unless @username.nil?
      concatenate(encode_string(@protocol_name),
                  [ @protocol_level ],
                  [ cflags ],
                  encode_short(@keep_alive),
                  encode_string(@client_id),
                  (@username ? encode_string(@username.not_nil!) : Bytes.new(0)),
                  (@password ? encode_string(@password.not_nil!) : Bytes.new(0)))
    end
  end

  class Connack < Packet

  end

  class Puback < Packet
    def initialize(@id : UInt16 = 0_u16,
                   @flags : Array(Bool) = [ false, false, false, false ],
                   @body_length : UInt64 = 0_u64)
    end

    def bytes : Bytes
      slice = Bytes.new(4)
      slice[0] = 64_u8; slice[1] = 2_u8
      slice[2] = (@id >> 8).to_u8
      slice[3] = @id.to_u8
      slice
    end
  end

  class Pubrec < Packet

  end

  class Pubrel < Packet
    def validate_flags
      return flags == [ false, true, false, false ]
    end
  end

  class Pubcomp < Packet

  end

  class Subscribe < Packet
    def initialize(@id : UInt16 = 0_u16,
                   @topic : String = "pub/test",
                   @flags : Array(Bool) = [ false, true, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def validate_flags
      return flags == [ false, true, false, false ]
    end

    def encode_body : Bytes
      concatenate(encode_short(@id), encode_string(@topic), [ 1_u8 ])
    end
  end

  class Suback < Packet

  end

  class Unsubscribe < Packet
    def validate_flags
      return flags == [ false, true, false, false ]
    end
  end

  class Unsuback < Packet

  end

  class Pingreq < Packet
    PING_DATA = slice_it "\xC0\x00"

    def bytes
      PING_DATA
    end
  end

  class Pingresp < Packet

  end

  class Disconnect < Packet

  end 

  PACKET_TYPES = [ nil,
    Packet::Connect,
    Packet::Connack,
    Packet::Publish,
    Packet::Puback,
    Packet::Pubrec,
    Packet::Pubrel,
    Packet::Pubcomp,
    Packet::Subscribe,
    Packet::Suback,
    Packet::Unsubscribe,
    Packet::Unsuback,
    Packet::Pingreq,
    Packet::Pingresp,
    Packet::Disconnect,
    nil
  ]
end

class String
  def self.new(arr : Array(UInt8))
    new(Slice.new(arr.size) { |i| arr[i] })
  end
end
