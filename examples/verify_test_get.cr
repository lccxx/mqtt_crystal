require "openssl"
require "../src/mqtt_crystal"

get_count, err_count = 0, 0

spawn {
  loop do
    sleep 1
    puts "get_count, err_count: #{[ get_count, err_count ]}"
  end
}

MqttCrystal::Client.new(host: "iot.eclipse.org").get("lccc/verify/test/#") { |t, m|
  get_count += 1
  err_count += 1 if OpenSSL::MD5.hash(t).map { |c| "%02x" % c }.join != m
}
