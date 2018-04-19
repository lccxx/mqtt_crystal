class MqttCrystal::Packet
  property flags, body_length

  def self.parse(bytes : Array(UInt8)) : Packet | Nil
    return nil if bytes.size == 0

    packet = Packet.create_from_header(bytes[0])
    return nil if packet.nil? || !packet.validate_flags

    remaining_length = packet.check_remaining_length(bytes[1,4])
    return nil if 1 + remaining_length[:pos] + remaining_length[:body_length] > bytes.size

    bytes.shift(1 + remaining_length[:pos])
    packet.parse_body(bytes.shift(remaining_length[:body_length]))

    packet
  end

  def self.create_from_header(byte : UInt8)
    type_id = (byte & 0xF0) >> 4
    packet_class = Packet::PACKET_TYPES[type_id]
    return if packet_class.nil?

    flags = Array(Bool).new()
    4.times { |i| flags << (byte & (1_u8 << i) != 0_u8) }

    packet_class.new(flags: flags)
  end

  def initialize(@flags : Array(Bool) = [ false, false, false, false ],
                 @body_length : UInt64 = 0_u64); end

  def validate_flags; true end

  def check_remaining_length(bytes : Array(UInt8)) : NamedTuple(pos: UInt8, body_length: UInt64)
    pos = 0_u8
    length = 0_u64
    multiplier = 1

    bytes.size.times { |i| pos += 1_u8
      digit = bytes[i]
      length += ((digit & 0x7F) * multiplier)
      multiplier *= 0x80
      break if (digit & 0x80).zero? || pos >= 4
    }

    { pos: pos, body_length: length }
  end

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

  def decode_short(buffer : Array(UInt8), index : Int32) : UInt16
    len = (buffer[index].to_u16 << 8) + buffer[index+1].to_u16
  end

  def _extract_string!(buffer : Array(UInt8), index : Int32 = 0) : String
    len = decode_short(buffer, index)
    result = String.new(buffer[index + 2, len])
    buffer.delete_at(index, len + 2)
    result
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

  def encode_string(strings : Array(String)) : Bytes
    size = strings.reduce(0_u16) { |acc, str| acc + str.bytes.size + 2 }
    slice = Bytes.new(size)
    index = 0
    strings.each do |str|
      bytes = str.bytes
      size_slice = encode_short(bytes.size.to_u16)
      slice[index] = size_slice[0]
      slice[index+1] = size_slice[1]
      index += 2
      bytes.each_with_index { |b, i| slice[index + i] = b }
      index += bytes.size
    end
    slice
  end

  def encode_header : UInt8
    ((type_id & 0x0F) << 4).to_u8 |
      (flags[3] ? 0x8_u8 : 0x0_u8) |
      (flags[2] ? 0x4_u8 : 0x0_u8) |
      (flags[1] ? 0x2_u8 : 0x0_u8) |
      (flags[0] ? 0x1_u8 : 0x0_u8)
  end

  def bytes : Bytes
    header = [ encode_header ]
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

  module PacketWithID
    # needed to override initialize on include
    macro included
      def initialize(@id : UInt16 = 0_u16,
                     @flags : Array(Bool) = [ false, false, false, false ],
                     @body_length : UInt64 = 0_u64); end
    end
    property id

    def bytes : Bytes
      slice = Bytes.new(4)

      slice[0] = encode_header
      slice[1] = 2_u8
      slice[2] = (@id >> 8).to_u8
      slice[3] = @id.to_u8
      slice
    end

    def parse_body(buffer : Array(UInt8))
      return nil unless buffer.size == 2
      @id = decode_short(buffer,0)
    end
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

    property clean_session, client_id, keep_alive, password, protocol_level,
      username, will_message, will_qos, will_retain, will_topic

    PROTOCOL            = "MQTT"
    CFLAG_USERNAME      = 0x80_u8
    CFLAG_PASSWORD      = 0x40_u8
    CFLAG_WILL_RETAIN   = 0x20_u8
    CFLAG_WILL          = 0x04_u8
    CFLAG_CLEAN_SESSION = 0x02_u8

    def initialize(@client_id : String = UUID.random.to_s,
                   @username : String | Nil = nil,
                   @password : String | Nil = nil,
                   @protocol_level : UInt8 = 0x04_u8,
                   @keep_alive : UInt16 = 15_u16,
                   @clean_session : Bool = true,
                   @will_retain : Bool = false,
                   @will_topic : String | Nil = nil,
                   @will_qos : UInt8 = 0_u8,
                   @will_message : String | Nil = nil,
                   @flags : Array(Bool) = [ false, false, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def encode_body : Bytes
      cflags = 0_u8
      cflags |= CFLAG_CLEAN_SESSION if @clean_session
      cflags |= CFLAG_WILL unless @will_topic.nil?
      cflags |= ((@will_qos & 0x03) << 3).to_u8
      cflags |= CFLAG_WILL_RETAIN if @will_retain
      cflags |= CFLAG_PASSWORD unless @password.nil?
      cflags |= CFLAG_USERNAME unless @username.nil?
      concatenate(encode_string(PROTOCOL),
                  [ @protocol_level ],
                  [ cflags ],
                  encode_short(@keep_alive),
                  encode_string(@client_id),
                  (@will_topic ? encode_string(@will_topic.not_nil!) : Bytes.new(0)),
                  (@will_message ? encode_string(@will_message.not_nil!) : Bytes.new(0)),
                  (@username ? encode_string(@username.not_nil!) : Bytes.new(0)),
                  (@password ? encode_string(@password.not_nil!) : Bytes.new(0)))
    end

    # byte[2..5] = 'MQTT'
    # byte[6] = Protocol requires 4 for MQTTv3.1.1
    # byte[7] = cflags
    #   7 - User Name Flag - is there a username 0x80
    #   6 - Password Flag - is there a password 0x40
    #   5 - Will Retain 0x20 - Sould the will be retained
    #   4 - Will QoS  0x10 - 0b10 (qos2),0b01 (qos1), 0b00 (qos0)
    #   3 - Will QoS  0x08
    #   2 - Will Flag 0x04 - Should there be a will message published
    #   1 - Clean Session 0x02
    #   0 - RESERVED (always 0) 0x00
    # byte[8,9] = Keepalive (seconds)
    # from ehre on
    # 2 byte length
    # N bytes payload, in order:
    #   Client Identifier, Will Topic, Will Message, User Name, Password
    def parse_body(buffer : Array(UInt8))
      # we're going to be destructive on the buffer so clone it
      buffer = buffer.clone
      return nil unless PROTOCOL.bytes == buffer[2,4]
      return nil unless buffer[6] == 4 # 4 == v3.1.1 because reasons

      cflags = buffer[7]
      @keep_alive = decode_short(buffer, 8)
      @clean_session = cflags.bit(1).zero? ? false : true
      @will_qos = cflags << 3 >> 6
      @will_retain = cflags.bit(5).zero? ? false : true

      # remove the first 10 items so we're up to encoded strings
      buffer.delete_at(0,10)
      @client_id = _extract_string!(buffer)
      # will flag
      if cflags.bit(2) == 1
        @will_topic = _extract_string!(buffer)
        @will_message = _extract_string!(buffer)
      end
      @username = _extract_string!(buffer) unless (cflags & CFLAG_USERNAME) == 0
      @password = _extract_string!(buffer) unless (cflags & CFLAG_PASSWORD) == 0
    end
  end

  class Connack < Packet
    property session_present, response

    # If this is a clean session, @session_present must be false
    # otherwise up to the implementation to decide if the session exists
    def initialize(@session_present = false,
                   @response = ConnackResponse::Accepted,
                   @flags : Array(Bool) = [ false, false, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def encode_body : Bytes
      slice = Bytes.new(2)
      slice[0] = session_present ? 1_u8 : 0_u8
      slice[1] = response.value
      slice
    end

    def parse_body(buffer : Array(UInt8))
      @session_present = buffer[0] == 1 ? true : false
      @response = ConnackResponse.new(buffer[1])
    end

  end

  # repsonse to a QoS 1 PUBLISH
  class Puback < Packet
    include PacketWithID
  end

  class Pubrec < Packet
    include PacketWithID
  end

  class Pubrel < Packet
    include PacketWithID

    def initialize(@id : UInt16 = 0_u16,
                   @flags : Array(Bool) = [ false, true, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def validate_flags
      return flags == [ false, true, false, false ]
    end
  end

  class Pubcomp < Packet
    include PacketWithID
  end

  class Subscribe < Packet
    property qos, topic

    def initialize(@id : UInt16 = 0_u16,
                   @topic : String = "pub/test",
                   @qos : UInt8 = 1_u8,
                   @flags : Array(Bool) = [ false, true, false, false ],
                   @body_length : UInt64 = 0_u64); end

    def validate_flags
      return flags == [ false, true, false, false ]
    end

    # Varialble Header
    # 2 bytes = packet_id
    # Payload
    # 2 bytes = topic_length
    # topic_length bytes = topic
    # 1 byte = qos

    def encode_body : Bytes
      concatenate(encode_short(@id), encode_string(@topic), [ @qos ])
    end

    def parse_body(buffer : Array(UInt8))
      return if buffer.size < 6 # minimum 1 char topic
      @id = (buffer[0].to_u16 << 8) + buffer[1].to_u16
      topic_length = (buffer[2] << 8) + buffer[3]
      return if topic_length + 5 != buffer.size
      @qos = buffer[-1]
      @topic = String.new(buffer[4, topic_length])
    end
  end

  class Suback < Packet
    property id, response
    def initialize(@id : UInt16 = 0_u16,
                   @response = SubackResponse::SuccessMaxQoS0,
                   @flags : Array(Bool) = [ false, false, false, false ],
                   @body_length : UInt64 = 0_u64); end

   def encode_body : Bytes
     concatenate(encode_short(@id), [@response.value])
   end

   def parse_body(buffer : Array(UInt8))
     return unless buffer.size == 3
     @id = (buffer[0].to_u16 << 8) + buffer[1].to_u16
     @response = SubackResponse.new(buffer[2])
   end


  end

  class Unsubscribe < Packet
    property id, topics

    def initialize(@id : UInt16 = 0_u16,
                   @topics : Array(String) = [] of String,
                   @flags : Array(Bool) = [false, true, false, false],
                   @body_length : UInt64 = 0_u64); end

    def encode_body : Bytes
      concatenate(encode_short(@id), encode_string(@topics))
    end

    def parse_body(buffer : Array(UInt8))
      buffer = buffer.clone
      @id = decode_short(buffer,0)
      buffer.delete_at(0,2)
      while buffer.size > 0
        @topics << _extract_string!(buffer)
      end
    end

    def validate_flags
      return flags == [ false, true, false, false ]
    end
  end

  class Unsuback < Packet
    include PacketWithID
  end

  class Pingreq < Packet
    PING_DATA = slice_it "\xC0\x00"

    def bytes
      PING_DATA
    end
  end

  class Pingresp < Packet
    PING_DATA = slice_it "\xD0\x00"

    def bytes
      PING_DATA
    end
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

  enum ConnackResponse : UInt8
    Accepted
    UnacceptableProtocolVersion
    IdentifierRejected
    ServerUnavailable
    BadUserOrPass
    NotAuthorized
  end

  enum SubackResponse : UInt8
    SuccessMaxQoS0
    SuccessMaxQoS1
    SuccessMaxQoS2
    Failure = 128
  end
end

class String
  def self.new(arr : Array(UInt8))
    new(Slice.new(arr.size) { |i| arr[i] })
  end
end
