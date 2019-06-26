require "./spec_helper"

describe MqttCrystal do
  it "packet types & flags" do
    packet = MqttCrystal::Packet.create_from_header(0_u8)
    packet.should be_a Nil

    packet = MqttCrystal::Packet.create_from_header(0x10_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Connect
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x20_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Connack
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x30_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x31_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x32_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, true, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x33_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, true, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x34_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, false, true, false]

    packet = MqttCrystal::Packet.create_from_header(0x35_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, false, true, false]

    packet = MqttCrystal::Packet.create_from_header(0x36_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, true, true, false]

    packet = MqttCrystal::Packet.create_from_header(0x37_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, true, true, false]

    packet = MqttCrystal::Packet.create_from_header(0x38_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, false, false, true]

    packet = MqttCrystal::Packet.create_from_header(0x39_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, false, false, true]

    packet = MqttCrystal::Packet.create_from_header(0x3a_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, true, false, true]

    packet = MqttCrystal::Packet.create_from_header(0x3b_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, true, false, true]

    packet = MqttCrystal::Packet.create_from_header(0x3c_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, false, true, true]

    packet = MqttCrystal::Packet.create_from_header(0x3d_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, false, true, true]

    packet = MqttCrystal::Packet.create_from_header(0x3e_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [false, true, true, true]

    packet = MqttCrystal::Packet.create_from_header(0x3f_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Publish
    packet.flags.should eq [true, true, true, true]

    packet = MqttCrystal::Packet.create_from_header(0x40_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Puback
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x50_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Pubrec
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x62_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Pubrel
    packet.flags.should eq [false, true, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x70_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Pubcomp
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x82_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Subscribe
    packet.flags.should eq [false, true, false, false]

    packet = MqttCrystal::Packet.create_from_header(0x90_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Suback
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0xa2_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Unsubscribe
    packet.flags.should eq [false, true, false, false]

    packet = MqttCrystal::Packet.create_from_header(0xb0_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Unsuback
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0xc0_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Pingreq
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0xd0_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Pingresp
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0xe0_u8).not_nil!
    packet.should be_a MqttCrystal::Packet::Disconnect
    packet.flags.should eq [false, false, false, false]

    packet = MqttCrystal::Packet.create_from_header(0xf0_u8)
    packet.should be_a Nil
  end

  it "connect packet send" do
    MqttCrystal::Packet::Connect.new(client_id: "test1", username: "liuchong", password: "lc123789").bytes
      .should eq slice_it "\x10%\x00\x04MQTT\x04\xC2\x00\x0F\x00\x05test1\x00\bliuchong\x00\blc123789"

    MqttCrystal::Packet::Connect.new(client_id: "tester", username: "yay", password: "boo", will_topic: "testy/test", will_retain: true, will_message: "Test message", will_qos: 1_u8).bytes
      .should eq slice_it "\x10\x36\x00\x04MQTT\x04\xEE\x00\x0f\x00\x06tester\x00\ntesty/test\x00\fTest message\x00\x03yay\x00\x03boo"

    MqttCrystal::Packet::Connect.new(client_id: "test1").bytes
      .should eq slice_it "\x10\x11\x00\x04MQTT\x04\x02\x00\x0F\x00\x05test1"

    MqttCrystal::Packet::Connect.new(client_id: "test12").bytes
      .should eq slice_it "\x10\x12\x00\x04MQTT\x04\x02\x00\x0F\x00\x06test12"

    MqttCrystal::Packet::Connect.new(client_id: "CR-79ee8b29-f2b3-4f69-9a8f-d0b3f30b849b").bytes
      .should eq slice_it "\x103\x00\x04MQTT\x04\x02\x00\x0F\x00'CR-79ee8b29-f2b3-4f69-9a8f-d0b3f30b849b"
  end

  it "connect packet recv" do
    packet = MqttCrystal::Packet.parse("\x10\x36\x00\x04MQTT\x04\xEC\x00\x3C\x00\x06tester\x00\ntesty/test\x00\fTest message\x00\x03yay\x00\x03boo".bytes)
    packet.should be_a MqttCrystal::Packet::Connect
    packet = packet.as(MqttCrystal::Packet::Connect)
    packet.client_id.should eq "tester"
    packet.will_retain.should be_true
    packet.will_qos.should eq 1_u8
    packet.clean_session.should be_false
    packet.keep_alive.should eq 60_u16
    packet.will_topic.should eq "testy/test"
    packet.will_message.should eq "Test message"
    packet.username.should eq "yay"
    packet.password.should eq "boo"

    packet = MqttCrystal::Packet.parse("\x10%\x00\x04MQTT\x04\xC2\x00\x0F\x00\x05test1\x00\bliuchong\x00\blc123789".bytes)
    packet.should be_a MqttCrystal::Packet::Connect
    packet = packet.as(MqttCrystal::Packet::Connect)
    packet.client_id.should eq "test1"
    packet.username.should eq "liuchong"
    packet.password.should eq "lc123789"

    packet = MqttCrystal::Packet.parse("\x10\x11\x00\x04MQTT\x04\x02\x00\x0F\x00\x05test1".bytes)
    packet.should be_a MqttCrystal::Packet::Connect
    packet = packet.as(MqttCrystal::Packet::Connect)
    packet.client_id.should eq "test1"
    packet.username.should be_nil
    packet.password.should be_nil

    packet = MqttCrystal::Packet.parse("\x10\x12\x00\x04MQTT\x04\x02\x00\x0F\x00\x06test12".bytes)
    packet.should be_a MqttCrystal::Packet::Connect
    packet = packet.as(MqttCrystal::Packet::Connect)
    packet.client_id.should eq "test12"
    packet.username.should be_nil
    packet.password.should be_nil
  end

  it "connack packet recv" do
    packet = MqttCrystal::Packet.parse([32_u8, 2_u8, 0_u8, 0_u8])
    packet.should be_a MqttCrystal::Packet::Connack
    packet = packet.as(MqttCrystal::Packet::Connack)
    packet.session_present.should be_false
    packet.response.should be_a MqttCrystal::Packet::ConnackResponse
    packet.response.should eq MqttCrystal::Packet::ConnackResponse::Accepted

    packet = MqttCrystal::Packet.parse([32_u8, 2_u8, 1_u8, 1_u8])
    packet.should be_a MqttCrystal::Packet::Connack
    packet = packet.as(MqttCrystal::Packet::Connack)
    packet.session_present.should be_true
    packet.response.should be_a MqttCrystal::Packet::ConnackResponse
    packet.response.should eq MqttCrystal::Packet::ConnackResponse::UnacceptableProtocolVersion
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
      .should eq slice_it "\x82\r\x00\x00\x00\bpub/test\x01"

    MqttCrystal::Packet::Subscribe.new(id: 1_u16, topic: "pub/t").bytes
      .should eq slice_it "\x82\n\x00\x01\x00\x05pub/t\x01"

    MqttCrystal::Packet::Subscribe.new(id: 535_u16, topic: "pub/t").bytes
      .should eq slice_it "\x82\n\x02\x17\x00\x05pub/t\x01"

    MqttCrystal::Packet::Subscribe.new(id: 1_u16, topic: "pub/CR-901c0c12-8b89-4fa1-9e2e-951cd47e5e88/test").bytes
      .should eq slice_it "\x825\x00\x01\x000pub/CR-901c0c12-8b89-4fa1-9e2e-951cd47e5e88/test\x01"
  end

  it "subscribe packet recv" do
    packet = MqttCrystal::Packet.parse("\x825\x00\x01\x000pub/CR-901c0c12-8b89-4fa1-9e2e-951cd47e5e88/test\x01".bytes)
    packet.should be_a MqttCrystal::Packet::Subscribe
    packet = packet.as(MqttCrystal::Packet::Subscribe)
    packet.qos.should eq 1_u8
    packet.topic.should eq "pub/CR-901c0c12-8b89-4fa1-9e2e-951cd47e5e88/test"

    packet = MqttCrystal::Packet.parse("\x82\n\x02\x17\x00\x05pub/t\x00".bytes)
    packet.should be_a MqttCrystal::Packet::Subscribe
    packet = packet.as(MqttCrystal::Packet::Subscribe)
    packet.qos.should eq 0_u8
    packet.topic.should eq "pub/t"
  end

  it "suback packet send" do
    MqttCrystal::Packet::Suback.new.bytes.should eq slice_it "\x90\x03\x00\x00\x00"
  end

  it "suback packet recv" do
    packet = MqttCrystal::Packet.parse("\x90\x03\x00\x01\x00".bytes)
    packet.should be_a MqttCrystal::Packet::Suback
    packet = packet.as(MqttCrystal::Packet::Suback)
    packet.id.should eq 1
    packet.response.should be_a MqttCrystal::Packet::SubackResponse
    packet.response.should eq MqttCrystal::Packet::SubackResponse::SuccessMaxQoS0

    packet = MqttCrystal::Packet.parse("\x90\x03\x00\x08\x80".bytes)
    packet.should be_a MqttCrystal::Packet::Suback
    packet = packet.as(MqttCrystal::Packet::Suback)
    packet.id.should eq 8
    packet.response.should be_a MqttCrystal::Packet::SubackResponse
    packet.response.should eq MqttCrystal::Packet::SubackResponse::Failure
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
    packet.id.should eq 0

    packet = MqttCrystal::Packet.parse("2\x1e\x00\bpub/test\x00\x020.9067131697364054".bytes)
    packet.should be_a MqttCrystal::Packet::Publish
    packet = packet.as MqttCrystal::Packet::Publish
    packet.topic.should eq "pub/test"
    packet.payload.should eq "0.9067131697364054"
    packet.qos.should eq 1
    packet.id.should eq 2
  end

  it "puback packet send" do
    MqttCrystal::Packet::Puback.new(id: 1_u16).bytes.should eq slice_it "@\x02\x00\x01"
    MqttCrystal::Packet::Puback.new(id: 455_u16).bytes.should eq slice_it "@\x02\x01\xC7"
    MqttCrystal::Packet::Puback.new(id: 65535_u16).bytes.should eq slice_it "@\x02\xFF\xFF"
  end

  it "puback packet recv" do
    [
      {id: 1_u16, as_string: "@\x02\x00\x01"},
      {id: 455_u16, as_string: "@\x02\x01\xC7"},
      {id: 65535_u16, as_string: "@\x02\xFF\xFF"},
    ].each do |params|
      packet = MqttCrystal::Packet.parse(params["as_string"].bytes)
      packet.should be_a MqttCrystal::Packet::Puback
      packet = packet.as(MqttCrystal::Packet::Puback).id.should eq params["id"]
    end
  end

  it "pubrec packet send" do
    MqttCrystal::Packet::Pubrec.new(id: 1_u16).bytes.should eq slice_it "\x50\x02\x00\x01"
    MqttCrystal::Packet::Pubrec.new(id: 455_u16).bytes.should eq slice_it "\x50\x02\x01\xC7"
    MqttCrystal::Packet::Pubrec.new(id: 65535_u16).bytes.should eq slice_it "\x50\x02\xFF\xFF"
  end

  it "pubrec packet recv" do
    [
      {id: 1_u16, as_string: "\x50\x02\x00\x01"},
      {id: 455_u16, as_string: "\x50\x02\x01\xC7"},
      {id: 65535_u16, as_string: "\x50\x02\xFF\xFF"},
    ].each do |params|
      packet = MqttCrystal::Packet.parse(params["as_string"].bytes)
      packet.should be_a MqttCrystal::Packet::Pubrec
      packet = packet.as(MqttCrystal::Packet::Pubrec).id.should eq params["id"]
    end
  end

  it "pubrel packet send" do
    MqttCrystal::Packet::Pubrel.new(id: 1_u16).bytes.should eq slice_it "\x62\x02\x00\x01"
    MqttCrystal::Packet::Pubrel.new(id: 455_u16).bytes.should eq slice_it "\x62\x02\x01\xC7"
    MqttCrystal::Packet::Pubrel.new(id: 65535_u16).bytes.should eq slice_it "\x62\x02\xFF\xFF"
  end

  it "pubrel packet recv" do
    [
      {id: 1_u16, as_string: "\x62\x02\x00\x01"},
      {id: 455_u16, as_string: "\x62\x02\x01\xC7"},
      {id: 65535_u16, as_string: "\x62\x02\xFF\xFF"},
    ].each do |params|
      packet = MqttCrystal::Packet.parse(params["as_string"].bytes)
      packet.should be_a MqttCrystal::Packet::Pubrel
      packet = packet.as(MqttCrystal::Packet::Pubrel).id.should eq params["id"]
    end
  end

  it "pubcomp packet send" do
    MqttCrystal::Packet::Pubcomp.new(id: 1_u16).bytes.should eq slice_it "\x70\x02\x00\x01"
    MqttCrystal::Packet::Pubcomp.new(id: 455_u16).bytes.should eq slice_it "\x70\x02\x01\xC7"
    MqttCrystal::Packet::Pubcomp.new(id: 65535_u16).bytes.should eq slice_it "\x70\x02\xFF\xFF"
  end

  it "pubcomp packet recv" do
    [
      {id: 1_u16, as_string: "\x70\x02\x00\x01"},
      {id: 455_u16, as_string: "\x70\x02\x01\xC7"},
      {id: 65535_u16, as_string: "\x70\x02\xFF\xFF"},
    ].each do |params|
      packet = MqttCrystal::Packet.parse(params["as_string"].bytes)
      packet.should be_a MqttCrystal::Packet::Pubcomp
      packet = packet.as(MqttCrystal::Packet::Pubcomp).id.should eq params["id"]
    end
  end

  it "unsubscribe packet send" do
    MqttCrystal::Packet::Unsubscribe.new(id: 1_u16, topics: %w(test/1 test/2)).bytes
      .should eq slice_it "\xa2\x12\x00\x01\x00\x06test/1\x00\x06test/2"
    MqttCrystal::Packet::Unsubscribe.new(id: 455_u16, topics: %w(test/1)).bytes
      .should eq slice_it "\xa2\x0A\x01\xC7\x00\x06test/1"
    MqttCrystal::Packet::Unsubscribe.new(id: 65535_u16, topics: %w(test/1)).bytes
      .should eq slice_it "\xa2\x0A\xFF\xFF\x00\x06test/1"
  end

  it "unsubscribe packet recv" do
    packet = MqttCrystal::Packet.parse("\xa2\x12\x00\x01\x00\x06test/1\x00\x06test/2".bytes)
    packet.should be_a MqttCrystal::Packet::Unsubscribe
    packet = packet.as MqttCrystal::Packet::Unsubscribe
    packet.topics.should eq %w(test/1 test/2)
    packet.id.should eq 1_u16

    packet = MqttCrystal::Packet.parse("\xa2\x0A\x01\xC7\x00\x06test/1".bytes)
    packet.should be_a MqttCrystal::Packet::Unsubscribe
    packet = packet.as MqttCrystal::Packet::Unsubscribe
    packet.topics.should eq %w(test/1)
    packet.id.should eq 455_u16

    packet = MqttCrystal::Packet.parse("\xa2\x0A\xFF\xFF\x00\x06test/1".bytes)
    packet.should be_a MqttCrystal::Packet::Unsubscribe
    packet = packet.as MqttCrystal::Packet::Unsubscribe
    packet.topics.should eq %w(test/1)
    packet.id.should eq 65535_u16
  end

  it "unsuback packet send" do
    MqttCrystal::Packet::Unsuback.new(id: 1_u16).bytes.should eq slice_it "\xB0\x02\x00\x01"
    MqttCrystal::Packet::Unsuback.new(id: 455_u16).bytes.should eq slice_it "\xB0\x02\x01\xC7"
    MqttCrystal::Packet::Unsuback.new(id: 65535_u16).bytes.should eq slice_it "\xB0\x02\xFF\xFF"
  end

  it "unsuback packet recv" do
    [
      {id: 1_u16, as_string: "\xB0\x02\x00\x01"},
      {id: 455_u16, as_string: "\xB0\x02\x01\xC7"},
      {id: 65535_u16, as_string: "\xB0\x02\xFF\xFF"},
    ].each do |params|
      packet = MqttCrystal::Packet.parse(params["as_string"].bytes)
      packet.should be_a MqttCrystal::Packet::Unsuback
      packet = packet.as(MqttCrystal::Packet::Unsuback).id.should eq params["id"]
    end
  end

  it "works" do
    config = File.tempfile("mosquitto.conf")
    config << "port 1883"
    config << "allow_anonymous true"

    ready = Channel(Bool).new
    done = Channel(Bool).new
    spawn do
      Process.run("mosquitto", ["-c", config.path], input: Process::Redirect::Close) { |p|
        loop {
          break if p.error.read_line.includes? "on port 1883"
        }
        ready.send true
        # Cleanup mosquitto after test finishes
        done.receive
        p.kill
      }
    end

    ready.receive
    client = MqttCrystal::Client.new(id: "CR-#{UUID.random.to_s}", host: "127.0.0.1")

    topic, payload = "pub/#{client.id}/test", (999 + rand(999)).times.map { rand(36).to_s(36) }.join

    publish_count = 7
    publish_max_wait = 200
    subscribed_max_wait = 10
    subscribed_wait_count = 0

    spawn {
      while !client.subscribed?
        sleep subscribed_max_wait.milliseconds
        subscribed_wait_count += 1
      end
      publish_count.times {
        sleep (rand(publish_max_wait) + 50).milliseconds
        client.publish(topic, payload)
      }
    }

    spawn {
      sleep (subscribed_max_wait * subscribed_wait_count +
             publish_max_wait * publish_count * 7 + 2000).milliseconds
      it "wait too long" {
        client.close
        false.should eq true
      }
    }

    get_count = 0

    client.get(topic) { |t, m|
      t.should eq topic
      m.should eq payload
      if (get_count += 1) == publish_count
        client.close
        client.connected?.should eq false
      end
    }

    done.send true
    config.delete
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
