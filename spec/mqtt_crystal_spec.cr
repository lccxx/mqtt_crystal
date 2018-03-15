require "./spec_helper"

describe MqttCrystal do
  it "connect packet send" do
    MqttCrystal::Packet::Connect.new(client_id: "test1", username: "liuchong", password: "lc123789").bytes
      .should eq slice_it "\x10%\x00\x04MQTT\x04\xC2\x00\x0F\x00\x05test1\x00\bliuchong\x00\blc123789"

    MqttCrystal::Packet::Connect.new(client_id: "test1").bytes
      .should eq slice_it "\x10\x11\x00\x04MQTT\x04\x02\x00\x0F\x00\x05test1"

    MqttCrystal::Packet::Connect.new(client_id: "test12").bytes
      .should eq slice_it "\x10\x12\x00\x04MQTT\x04\x02\x00\x0F\x00\x06test12"

    MqttCrystal::Packet::Connect.new(client_id: "CR-79ee8b29-f2b3-4f69-9a8f-d0b3f30b849b").bytes
      .should eq slice_it "\x103\x00\x04MQTT\x04\x02\x00\x0F\x00'CR-79ee8b29-f2b3-4f69-9a8f-d0b3f30b849b"
  end

  it "connack packet recv" do
    MqttCrystal::Client.new(IO::Memory.new(" \x02\x00\x00")).read_packet
      .should be_a MqttCrystal::Packet::Connack
  end

  it "ping packet send" do
    MqttCrystal::Packet::Pingreq.new.bytes.should eq slice_it "\xC0\x00"
  end

  it "pingresp packet recv" do
    MqttCrystal::Client.new(IO::Memory.new("\xD0\x00")).read_packet
      .should be_a MqttCrystal::Packet::Pingresp
  end

  it "subscribe packet send" do
    MqttCrystal::Packet::Subscribe.new(topic: "pub/test").bytes
      .should eq slice_it "\x82\r\x00\x00\x00\bpub/test\x00"

    MqttCrystal::Packet::Subscribe.new(id: 1_u16, topic: "pub/t").bytes
      .should eq slice_it "\x82\n\x00\x01\x00\x05pub/t\x00"

    MqttCrystal::Packet::Subscribe.new(id: 535_u16, topic: "pub/t").bytes
      .should eq slice_it "\x82\n\x02\x17\x00\x05pub/t\x00"

    MqttCrystal::Packet::Subscribe.new(id: 1_u16, topic: "pub/CR-901c0c12-8b89-4fa1-9e2e-951cd47e5e88/test").bytes
      .should eq slice_it "\x825\x00\x01\x000pub/CR-901c0c12-8b89-4fa1-9e2e-951cd47e5e88/test\x00"
  end

  it "suback packet recv" do
    MqttCrystal::Client.new(IO::Memory.new("\x90\x03\x00\x01\x00")).read_packet
      .should be_a MqttCrystal::Packet::Suback

    MqttCrystal::Client.new(IO::Memory.new("\x90\x03\x00\x00\x00")).read_packet
      .should be_a MqttCrystal::Packet::Suback
  end

  it "publish packet send" do
    MqttCrystal::Packet::Publish.new(topic: "pub/test", payload: "test").bytes
      .should eq slice_it "0\x0E\x00\bpub/testtest"
  end

  it "puback packet send" do
    MqttCrystal::Packet::Puback.new(id: 1_u16).bytes.should eq slice_it "@\x02\x00\x01"
    MqttCrystal::Packet::Puback.new(id: 455_u16).bytes.should eq slice_it "@\x02\x01\xC7"
    MqttCrystal::Packet::Puback.new(id: 65535_u16).bytes.should eq slice_it "@\x02\xFF\xFF"
  end

  it "works" do
    client = MqttCrystal::Client.new(id: "CR-#{UUID.random.to_s}")

    client.connect("172.17.0.1")
      .should be_a MqttCrystal::Packet::Connack

    client.ping.should be_a MqttCrystal::Packet::Pingresp

    topic = "pub/#{client.id}/test"
    message = rand.to_s

    client.subscribe(topic)
      .should be_a MqttCrystal::Packet::Suback 

    9.times {
      client.publish(topic, message)
      packet = client.read_packet
      packet.should be_a MqttCrystal::Packet::Publish
      packet = packet.as(MqttCrystal::Packet::Publish)
      packet.topic.should eq topic
      packet.payload.should eq message
    }

    spawn { client.keep_alive }
  end
end
