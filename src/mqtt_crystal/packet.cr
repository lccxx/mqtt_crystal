class MqttCrystal::Packet
  ATTR_DEFAULTS = { version: "3.1.0", id: 0, body_length: 0_u64 }

  property flags, body_length

  def self.create_from_header(byte : UInt8)
    type_id = ((byte & 0xF0) >> 4)
    packet_class = MqttCrystal::Packet::PACKET_TYPES[type_id]
    return if packet_class.nil?

    flags = Array(Bool).new()
    4.times { |i| flags << (byte & (2**i) != 0) }

    packet_class.new(flags: flags)
  end

  def initialize(@flags : Array(Bool) = [ false, false, false, false ],
                 @body_length : UInt64 = 0_u64); end

  def type_id : Int32
    PACKET_TYPES.index(self.class).not_nil!
  end

  def parse_body(buffer : Bytes)

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

    slice = Bytes.new(header.size)
    header.map_with_index { |n, i| slice[i] = n }

    concatenate(slice, body)
  end

  class Publish < MqttCrystal::Packet
    def initialize(@topic : String = "pub/test",
                   @payload : String = "test",
                   @flags : Array(Bool) = [ false, false, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def bytes
      slice_it "0\x0E\x00\b#{@topic}#{@payload}".bytes
    end
  end

  class Connect < MqttCrystal::Packet
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

  class Connack < MqttCrystal::Packet

  end

  class Puback < MqttCrystal::Packet
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

  class Pubrec < MqttCrystal::Packet

  end

  class Pubrel < MqttCrystal::Packet

  end

  class Pubcomp < MqttCrystal::Packet

  end

  class Subscribe < MqttCrystal::Packet
    def initialize(@id : UInt16 = 0_u16,
                   @topic : String = "pub/test",
                   @flags : Array(Bool) = [ false, true, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def encode_body : Bytes
      concatenate(encode_short(@id), encode_string(@topic), [ 0_u8 ])
    end
  end

  class Suback < MqttCrystal::Packet

  end

  class Unsubscribe < MqttCrystal::Packet

  end

  class Unsuback < MqttCrystal::Packet

  end

  class Pingreq < MqttCrystal::Packet
    PING_DATA = slice_it "\xC0\x00"

    def bytes
      PING_DATA
    end
  end

  class Pingresp < MqttCrystal::Packet

  end

  class Disconnect < MqttCrystal::Packet

  end 

  PACKET_TYPES = [ nil,
    MqttCrystal::Packet::Connect,
    MqttCrystal::Packet::Connack,
    MqttCrystal::Packet::Publish,
    MqttCrystal::Packet::Puback,
    MqttCrystal::Packet::Pubrec,
    MqttCrystal::Packet::Pubrel,
    MqttCrystal::Packet::Pubcomp,
    MqttCrystal::Packet::Subscribe,
    MqttCrystal::Packet::Suback,
    MqttCrystal::Packet::Unsubscribe,
    MqttCrystal::Packet::Unsuback,
    MqttCrystal::Packet::Pingreq,
    MqttCrystal::Packet::Pingresp,
    MqttCrystal::Packet::Disconnect,
    nil
  ]
end
