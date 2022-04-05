atio-publishers
===============================================================================

**A set of utility classes that assist in publishing streaming data to the atio 
trading engine**


Currently streaming from websockets is supported with the following abstract
classes:


- ``atio_publishers.websockets.BaseWSClient``: Handles how data is retrieved
  from the socket stream, and then passed to the worker and onwards to the
  publisher.

- ``atio_publishers.websockets.Worker``: Handles the processing of the raw
  websocket message feed. This happens in a forked process to prevent blocking
  the asyncio event loop in th main thread.

- ``atio_publishers.websockets.Publisher``: Handles the sending of the
  processed data to a redis pubsub channel. Again, this happens in a seperate
  process for maximum performance.

