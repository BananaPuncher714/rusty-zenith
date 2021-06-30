# Rusty Zenith
A basic internet radio server written in rust that uses Icecast's HTTP protocol. Inspired by [Icecast 2](https://icecast.org/).

## About
This program was created to allow updating and sending of a stream url in the icy metadata sent to compatible listeners. It currently serves `SOURCE` requests, listeners, and updating of mount metadata nearly identical to Icecast. [VLC](https://www.videolan.org/) was used as a source for testing.

## Features
- Supports Icecast `SOURCE` requests
- Supports Icecast metadata updates
- Supports `Icy-Metadata`
- Supports `StreamTitle` and `StreamUrl`
- Configurable users and passwords
- Burst on connect

## Config
<details>
  <summary>Default configuration (Click to expand)</summary> 
  
```json
{
  "port": 8000,
  "metaint": 16000,
  "server_id": "Rusty Zenith 0.1.0",
  "admin": "admin@localhost",
  "host": "localhost",
  "location": "1.048596",
  "limits": {
    "clients": 400,
    "sources": 4,
    "queue_size": 102400,
    "burst_size": 65536,
    "header_timeout": 15000,
    "source_timeout": 10000
  },
  "users": [
    {
      "username": "admin",
      "password": "hackme"
    },
    {
      "username": "source",
      "password": "hackme"
    }
  ]
}
```

</details>

- `port`: The port to use for serving requests and accepting clients
- `metaint`: The interval in bytes between `Icy_Metadata` updates to compatible clients
- `server_id`: The name of the server
- `admin`: The contact information for the server
- `host`: Public facing domain/url
- `location`: Geographic location
- `limits.clients`: Maximum number of concurrent listeners supported by the server. Does not include static accesses, such as requests to gather stats. This is the max number of listeners for the entire server, not per mountpoint.
- `limit.sources`: Maximum number of connected sources supported by the server.
- `limits.queue_size`: **(Taken from the Icecast docs)**
  > This is the maximum size (in bytes) of a client (listener) queue. A listener may temporarily lag behind due to network congestion and in this case an internal queue is maintained for each listener. If the queue grows larger than this config value, then the listener will be removed from the stream.

- `limits.source_timeout`: Setting this value to `0` disables burst-on-connect. **(Taken from the Icecast docs)**
  > The burst size is the amount of data (in bytes) to burst to a client at connection time. Like burst-on-connect, this is to quickly fill the pre-buffer used by media players. The default is 64 kbytes which is a typical size used by most clients so changing it is not usually required. This setting applies to all mountpoints.

- `limits.header_timeout`: Uses milliseconds instead of seconds. **(Taken from the Icecast docs)**
  > The maximum time (in milliseconds) to wait for a request to come in once the client has made a connection to the server. In general this value should not need to be tweaked.

- `limits.source_timeout`: Uses milliseconds instead of seconds. **(Taken from the Icecast docs)**
  > If a connected source does not send any data within this timeout period (in milliseconds), then the source connection will be removed from the server.

- `users`: A list of username and passwords that can create sources or execute admin requests.


## TODO
- Add stats
- Add a logging system
- Implement the PUT request
- Add a fallback mount
- Add a permission system for users
- Add a separate base directory for streams
- Add per-mountpoint settings
- Add a relay system?