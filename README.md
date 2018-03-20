# mqtt_crystal

pure crystal mqtt client

## Installation

Add this to your application's `shard.yml`:

```yaml
dependencies:
  mqtt_crystal:
    github: liu-chong/mqtt_crystal
    branch: master
```

## Usage

```crystal
require "mqtt_crystal"

client = MqttCrystal::Client.new(url: "mqtt://iot.eclipse.org") # id, host, port, username, password, url

# you may need spawn it
client.get("lccc/teeest/#") { |t, m|
  puts "#{t}, #{m}"
}

client.publish("lccc/teeest/topiiic", "test payload xxxxyyyyyy")
```

## Development

git clone https://github.com/liu-chong/mqtt_crystal.git

cd mqtt_crystal

crystal spec

## Contributing

1. Fork it ( https://github.com/liu-chong/mqtt_crystal/fork )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create a new Pull Request

## Contributors

- [[liu-chong]](https://github.com/liu-chong) Liu Chong - creator, maintainer
