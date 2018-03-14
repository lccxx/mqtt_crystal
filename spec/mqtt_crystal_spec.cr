require "./spec_helper"

describe MqttCrystal do
  it "works" do
    client = MqttCrystal::Client.new("CR-#{UUID.random.to_s}")
    client.connect("172.17.0.1")
    topic = "pub/#{client.id}/test"
    message = rand.to_s
    client.get(topic) { |t, m|
      t.should eq topic
      m.should eq message
    }
    sleep 0.5
    client.publish(topic, message)
    sleep 0.5
  end
end
