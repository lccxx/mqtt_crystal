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

client = MqttCrystal::Client.new(id: "test1", host: "172.17.0.1")

client.get("pub/#") { |t, m|
  puts "#{t}, #{m}"
}

client.publish("pub/test1", "test payload xxxxyyyyyy")
```

new -> connect -> subscribe -> get -> publish

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
