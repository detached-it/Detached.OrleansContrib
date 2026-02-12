# Detached Orleans Libraries

This repository contains a collection of Orleans libraries that are not part of the main Orleans distribution.

> :memo: **NOTE:** these are "playground projects" and should not be used in production until you've thoroughly tested their integration. I've provided near 100% test coverage, but as far as performance is concerned I doubt it'll stand up to any high load.

## Libraries

- **Detached.OrleansContrib.Streaming.GrainStream**: A simple orleans Stream provider using grains for persistence and delivery.
  - I believe it is fully functional, but I still need to do some stress/perforamance testing.
  - Consider using one of the official Azure-based stream providers if you are running on azure. If you're on another platform it seems that you're out of luck...
  - I created this as a quick way to get an on-prem storage provider without any new dependencies/infrastructure, so there should be better options available.

## Getting Started

See the individual library READMEs for getting started instructions.
