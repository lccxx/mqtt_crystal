class MqttCrystal::Client
  property id

  def initialize(@id : String); end  

  def connect(@host : String = "127.0.0.1", @port : Int32 = 1883)
  end

  def get(topic : String)
    yield "t", "m"
  end

  def publish(topic : String, message : String)
  end
end
