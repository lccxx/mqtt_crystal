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
    MqttCrystal::Packet.parse([ 32_u8, 2_u8, 0_u8, 0_u8 ])
      .should be_a MqttCrystal::Packet::Connack
  end

  it "ping packet send" do
    MqttCrystal::Packet::Pingreq.new.bytes.should eq slice_it "\xC0\x00"
  end

  it "pingresp packet recv" do
    MqttCrystal::Packet.parse("\xD0\x00".bytes)
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
    MqttCrystal::Packet.parse("\x90\x03\x00\x01\x00".bytes)
      .should be_a MqttCrystal::Packet::Suback

    MqttCrystal::Packet.parse("\x90\x03\x00\x00\x00".bytes)
      .should be_a MqttCrystal::Packet::Suback
  end

  it "publish packet send" do
    MqttCrystal::Packet::Publish.new(topic: "pub/test", payload: "test").bytes
      .should eq slice_it "0\x0E\x00\bpub/testtest"

    MqttCrystal::Packet::Publish.new(id: 1_u16, qos: 1_u8, topic: "pub/test", payload: "teeest").bytes
      .should eq slice_it "2\x12\x00\bpub/test\x00\x01teeest"

    MqttCrystal::Packet::Publish.new(id: 2_u16, qos: 1_u8, topic: "pub/test", payload: "teeest").bytes
      .should eq slice_it "2\x12\x00\bpub/test\x00\x02teeest"
  end

  it "publish packet recv" do
    packet = MqttCrystal::Packet.parse("0\x11\x00\bpub/testtestttt".bytes)
    packet.should be_a MqttCrystal::Packet::Publish
    packet = packet.as(MqttCrystal::Packet::Publish)
    packet.topic.should eq "pub/test"
    packet.payload.should eq "testttt"
    packet.qos.should eq 0

    packet = MqttCrystal::Packet.parse("0\x17\x00\x0epub/testmqttjstesssst".bytes)
    packet.should be_a MqttCrystal::Packet::Publish
    packet = packet.as(MqttCrystal::Packet::Publish)
    packet.topic.should eq "pub/testmqttjs"
    packet.payload.should eq "tesssst"
    packet.qos.should eq 0
  end

  it "puback packet send" do
    MqttCrystal::Packet::Puback.new(id: 1_u16).bytes.should eq slice_it "@\x02\x00\x01"
    MqttCrystal::Packet::Puback.new(id: 455_u16).bytes.should eq slice_it "@\x02\x01\xC7"
    MqttCrystal::Packet::Puback.new(id: 65535_u16).bytes.should eq slice_it "@\x02\xFF\xFF"
  end

  it "works" do
    client = MqttCrystal::Client.new(id: "CR-#{UUID.random.to_s}", host: "172.17.0.1")

    topic, payload = "pub/#{client.id}/test", rand.to_s

    spawn {
      99.times {
        sleep 0.01
        client.publish(topic, payload)
      }
    }

    spawn {
      sleep 0.5.seconds
      client.close
      client.connected?.should eq false
    }

    client.get(topic) { |t, m|
      it "get" {
        t.should eq topic
        m.should eq payload
      }
    }
  end

  it "mqtt connect url" do
    client = MqttCrystal::Client.new(url: "mqtt://172.17.0.1")
    client.host.should eq "172.17.0.1"
    client.port.should eq 1883_u16
    client.username.should be_a Nil
    client.password.should be_a Nil

    client = MqttCrystal::Client.new(url: "mqtt://172.17.0.1:1234")
    client.host.should eq "172.17.0.1"
    client.port.should eq 1234_u16
    client.username.should be_a Nil
    client.password.should be_a Nil

    client = MqttCrystal::Client.new(url: "mqtt://uuuuu:pppppp@iot.eclipse.org:1234")
    client.host.should eq "iot.eclipse.org"
    client.port.should eq 1234_u16
    client.username.should eq "uuuuu"
    client.password.should eq "pppppp"
  end
end
