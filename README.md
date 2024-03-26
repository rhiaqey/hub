# Hub

![Hub](https://github.com/rhiaqey/hub/actions/workflows/pr_merge.yml/badge.svg)

Early development stage

## Redis streams example

[Redis streams](https://github.com/redis-rs/redis-rs/blob/main/redis/examples/streams.rs)

## Settings

```json
{
	"Security": {
     	"ApiKeys": [
          {
           	"ApiKey": "some_api_key",
            "Host": "some_host",
            "IPs": {
              "Blacklisted": [
                "192.168.0.0"
              ]
            }
          }
        ] 
    }
}
```

## TODO

* Move all crud operations to redis_rs
* Introduce new actions for RPCMessage + RPCMessageData
* All crud operations will have to publish an action to hub_raw_to_hub_clean_pubsub_topic similar to update_settings_for_hub
* Handle each new action in every hub
