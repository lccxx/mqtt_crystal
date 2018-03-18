require "openssl"
require "../src/mqtt_crystal"

client = MqttCrystal::Client.new(host: "172.17.0.1").connect

pub_count, get_count, err_count = 0, 0, 0

spawn {
  loop {
    next if !client.connected?
    topic = "pub/verify/test/#{rand}"
    client.publish(topic, OpenSSL::MD5.hash(topic).map { |c| "%02x" % c }.join)
    pub_count += 1
    sleep (rand / 100).seconds
  }
}

spawn {
  loop do
    sleep 1
    puts "pub_count, get_count, err_count: #{[ pub_count, get_count, err_count ]}"
  end
}

client.get("pub/verify/test/#") { |t, m|
  get_count += 1
  err_count += 1 if OpenSSL::MD5.hash(t).map { |c| "%02x" % c }.join != m
}
