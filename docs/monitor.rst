Monitor Server
==============

Monitor Server is component responsible for providing infrastructure necessary
for multiple redundant component execution. Additionally, Monitor Server
provides centralized registry of running components in single system which can
be used for service discovery.

.. warning::

    Monitor Server functionality is defined under assumptions:

        * components running as part of single system are trusted and
          implemented according to specifications
        * communication channels between components on single computing
          node are reliable

    Redundancy mechanisms provided by Monitor Server do not provide
    reliability in case of possible component malfunctions due to
    implementation errors.


Running
-------

By installing Monitor Server from `hat-monitor` package, executable
`hat-monitor` becomes available and can be used for starting this component.

    .. program-output:: python -m hat.monitor.server --help

Additionally, `hat-monitor` package provides implementation of library which
can be used as basis for communication between components and Monitor Server.
This library is available in `hat.monitor.client` module.


Communication model
-------------------

Monitor Server provides n-node redundancy architecture based on
chatter protocol (structure of communication messages is
defined in `HatMonitor` package). It is based on server-client communication
between components and monitor server. There also exists horizontal peer
communication between multiple monitor servers which enables forming of
single system based on multiple distributed computing nodes. It is assumed
that, for each system, single computing node runs single instance of Monitor
Server and unlimited number of components that connect to local Monitor Server.
Each node's Monitor Server establishes communication with other nodes' Monitor
Server through single master Monitor Server. Hierarchy of Monitor Servers
which can perform functionality of master is configured statically with
configuration options available to each Monitor Server on its startup.

Entities participating in 'vertical' communication between components and local
monitor server:

    * Client

        Component proclaiming its existence to the server, discovering
        other components and participating in redundancy algorithm.

    * Server

        Local monitor server providing global components state to all local
        clients and providing user interface.

Entities participating in 'horizontal' communication between Monitor Servers:

    * Master

        Monitor Server responsible for execution of the redundancy algorithm
        and notifying other Monitor Servers about the current global state of
        the entire system.

    * Slave

        Monitor Server responsible for notifying master about its current local
        state and delegating master's notifications to its server entity.

Monitor Server uses two different independent listening sockets for
client-server and master-slave communication.

If we represent components with `Cn` and Monitor Servers with `Mn`, where
master hierarchy of `M1 > ... > Mn` is presumed, an example of a single system
monitor communication can be viewed as:

    .. graphviz::
        :align: center

        graph {
            bgcolor=transparent;
            layout=neato;
            node [fontname="Arial"];
            {
                node [shape=box];
                M1; M2; M3; M4;
            }
            {
                node [shape=oval];
                C1; C2; C3; C4; C5; C6; C7; C8; C9; C10; C11; C12;
            }
            {
                edge [dir=forward, style=bold, len=2];
                M2 -- M1;
                M3 -- M1;
                M4 -- M1;
            }
            {
                edge [dir=forward, style="bold,dashed", len=2];
                M3 -- M2;
                M4 -- M2;
                M4 -- M3;
            }
            {
                edge [dir=both, arrowsize=0.5];
                M1 -- C1;
                M1 -- C2;
                M1 -- C3;
                M2 -- C4;
                M2 -- C5;
                M2 -- C6;
                M3 -- C7;
                M3 -- C8;
                M3 -- C9;
                M4 -- C10;
                M4 -- C11;
                M4 -- C12;
            }
        }


Component information
---------------------

Component information is basic structure of properties that describe each
component included in system. It is initially created on local Monitor Server
and later updated by master Monitor Server. Collection of all components
information associated with clients connected to local Monitor Server and
calculated by local Monitor Server is called local state. Collection of all
components information in single system calculated by master Monitor server is
called global state. Each Monitor Server provides global state to its local
clients.

Properties included in a component information:

    * `cid`

        Component id assigned to client by its local Monitor Server.

    * `mid`

        Monitor id identifying local Monitor Server (assigned to local Monitor
        Server by master). Value ``0`` indicates Monitor Server which is master
        or is not connected to remote master.

    * `name`

        User provided identifier of component. This entry is used for
        UI presentation purposes, logging and rank caching. It is recommended
        to use unique identifiers for each component instance. This property
        is assigned by client.

    * `group`

        String identifier by which components are grouped while blessing
        calculation algorithm is applied (see `Blessing algorithm`_). This
        property is assigned by client.

    * `data`

        JSON serializable data representing arbitrary information that
        correspond to the component. This property is assigned by client.

    * `rank`

        Component's rank - used by `Blessing algorithm`_. This property is
        initially assigned by local Monitor Server. Changes of this property
        value is available as part of local Monitor Server's UI.

    * `blessing_req`

        Blessing request assigned and changed exclusively by master
        Monitor Server (see `Component lifetime`_). It consists of two optional
        properties:

        - `token` is optional number, used as unique token with the purpose
          of assigning blessing to the component for its primary functionality.
          When `token` is ``None``, it means component is not blessed.
          Hereafter this `token` is called request `token`.

        - `timestamp` is optional number that represents Unix epoch
          timestamp. It is strongly related to request `token` since
          corresponds to point in time when master Monitor Server assigned
          request `token` to the component. When `token` is ``None``, master
          also sets `timestamp` to``None``.

    * `blessing_res`

        Blessing response assigned and changed exclusively by client
        (see `Component lifetime`_). It consists of two properties:

        - `token` is optional number, used as unique token as a client's
          response to master's blessing request `token`. When response `token`
          is set to exactly the same value as the request `token`, it means
          component is active, that is, it started providing its primary
          functionality. When component is no more active, it revokes this
          `token` by setting it to ``None``. Hereafter this `token` is called
          response `token`.

        - `ready` is boolean indicating whether component is ready to provide
          its primary functionality.


Master slave communication
--------------------------

Horizontal communication between Monitor Servers is hierarchically ordered.
Each Monitor Server knows its superiors' addresses. If ``M1 > M2 > M3``,
then ``M1`` doesn't know any other monitor address; ``M2`` knows the address
of ``M1``; ``M3`` knows addresses of ``M1`` and ``M2`` in that order.

Each Monitor Server's configuration contains zero or more other Monitor
Server addresses. These other servers are "superior" to the monitor server. A
monitor server will always try to maintain an active connection with exactly
one of its superiors. The addresses list is ordered by priority meaning that if
the Monitor Server isn't connected to a superior, it tries to connect to the
first monitor server in the list with `connect_timeout`. If the connection
fails, it tries the second one and so on. If it can't connect to any of its
superiors, it waits for `connect_retry_delay` and retries again from the first
monitor server in the list. It will retry `connect_retry_count` times before
it can proclaim itself as master. The connecting to master process continues
until the Monitor Server connects to its first superior, even if the Monitor
Server is master or connection to some other superior is established.
Connection parameters `connect_timeout`, `connect_retry_delay` and
`connect_retry_count` are defined with configuration.

Once a slave Monitor Server connects to the Master Monitor server it sends its
local state to the master and keeps notifying the master about any change in
its local state while the connection is active. The master gathers all local
states and generates its global state which it then transmits to all its
slaves and keeps notifying them of any change. Global state contains information
from all components received from local states except for those where
component's name or group are not set. Master also identifies each
Monitor Server with unique monitor identifier (`mid`) which is provided to
slave together with global state. It is important to note that only master
Monitor Server calculates blessing request `blessing_req` for each component.

Every Monitor Server is responsible for starting master listening socket
immediately after startup. While Monitor Server isn't operating in master mode,
all connections made to master listening socket will be closed immediately
after their establishment - this behavior will indicate to connecting Monitor
Server that its superior is not currently master.

Messages used in master slave communications are defined in `HatMonitor` SBS
module (see `Chatter messages`_). These messages are:

    +--------------------+----------------------+-----------+
    |                    | Conversation         |           |
    | Message            +-------+------+-------+ Direction |
    |                    | First | Last | Token |           |
    +====================+=======+======+=======+===========+
    | MsgSlave           | T     | T    | T     | s |arr| m |
    +--------------------+-------+------+-------+-----------+
    | MsgMaster          | T     | T    | T     | m |arr| s |
    +--------------------+-------+------+-------+-----------+

where `s` |arr| `m` represents slave to master communication and `m` |arr| `s`
represents master to slave communication. When new connection is established,
master should immediately associate new `mid` with connection and wait for
`MsgSlave` sent by slave. After master receives `MsgSlave` and calculates
new global state, it will send `MsgMaster` to slave. Once initial exchange
of `MsgSlave` followed by `MsgMaster` finished, each communicating entity
(master or slave) should send new state message (`MsgMaster` or `MsgSlave`) if
any data obtained from `MsgSlave` or `MsgMaster` changes. Sending of
`MsgMaster` and `MsgSlave` should be implemented independent of receiving
messages from associated entity.


Server client communication
---------------------------

Vertical communication between client and server enables bidirectional
asynchronous exchange of component information data. Client is responsible
for providing `name`, `group`, `data` and `blessing_res` properties initially
and on every change. Server provides global state to each connected client and
each client's component id (`cid`) and monitor id (`mid`). If any part of
state available to server changes (including token changes), server sends
updated state to all clients. Client can also request change for information
provided to server at any time.

Messages used in server client communications are defined in `HatMonitor` SBS
module (see `Chatter messages`_). These messages are:

    +--------------------+----------------------+-----------+
    |                    | Conversation         |           |
    | Message            +-------+------+-------+ Direction |
    |                    | First | Last | Token |           |
    +====================+=======+======+=======+===========+
    | MsgClient          | T     | T    | T     | c |arr| s |
    +--------------------+-------+------+-------+-----------+
    | MsgServer          | T     | T    | T     | s |arr| c |
    +--------------------+-------+------+-------+-----------+

where `c` |arr| `s` represents client to server communication and `s` |arr|
`c` represents server to client communication. When new connection is
established, each communicating entity (server or client) server immediately
sends initial state message (`MsgServer` or `MsgClient`) and should send new
state messages when any data obtained from `MsgServer` or `MsgClient` changes.
Sending of `MsgServer` and `MsgClient` should be implemented independent of
receiving messages from associated entity. Implementation of server should not
be dependent on receiving initial `MsgClient` and should continue sending
`MsgServer` on every state change even if no `MsgClient` is received.

Server always sends last known global state calculated by master monitor
server (even in case when connection to master is not established).


Component lifetime
------------------

For most components, connection to local Monitor Server is mandatory for
providing implemented functionality. Because of this, component usually connects
to local Monitor Server during startup and preserves this active connection
during entire component run lifetime. If this connection is closed for any
reason, process also terminates. This behavior is not mandated.

Components which connect to Monitor Server participate in redundancy
supervised by master Monitor Server. Redundancy utilizes two tokens, the one
from `blessing_req`, said as request `token`, and the other from
`blessing_res`, said as response `token`:

    * request `token`

        This token is controlled exclusively by master Monitor Server.
        Master gives "blessing for work" to a component by setting this
        token to an integer number. On the other hand, it revokes blessing
        by setting this token to ``None``. If connection to master is not
        established, token's value equals to ``None``.

    * response `token`

        This token is controlled exclusively by client. Upon receiving request
        `token` component starts providing its functionality and sets
        response `token` to match the request `token` in order to signalize
        its activity. If, at any time, component stops its activity, it revokes
        token by setting it to ``None``.

While component information has request and response tokens with the same
same value, it means component is active. If, at any time,
component losses blessing, that is, master revokes request `token`, component
starts with stopping its activity. When component activity is stopped it
indicates it by revoking the response `token`.

On behalf of `ready` property of `blessing_res`, each component informs
whether it is ready to provide its functionality based on global state provided
by local Monitor Server. While component is not ready, one does not expect that
it gets the request `token`. In any case, component that is not `ready` never
sets its response `token`.

Responsibility of each Monitor Server is to cache last known `blessing_req`
associated with it's local components. This information is provided to
master Monitor Server thus enabling transfer of `blessing_req` state in cases
of master switchover procedures.


Blessing algorithm
------------------

Blessing algorithm determines value of each component's request `token` and
associated `timestamp`. This calculation is performed on master Monitor Server
and should be executed each time any part of global state changes. This
calculation should be integrated part of state change and thus provide global
state consistency. That is, one can say that master Monitor Server blessed a
component when it set its request `token` to an integer value.

Monitor Server implements multiple algorithms for calculating value of request
token. Each component `group` can have associated different blessing algorithm
and all groups that don't have associated blessing algorithm use default
algorithm. Group's associated algorithms and default algorithm are provided
to Monitor Server as configuration parameters during its startup.

Calculation of request `token` values is based only on previous global state
and new changes that triggered execution of blessing algorithm.

Currently supported algorithms:

    * BLESS_ALL

        This simple algorithm provides blessing to all components in associated
        group that are ready (`ready` flag of `blessing_res` is ``True``).
        Blessing is revoked only when `ready` flag is set to ``False``.

    * BLESS_ONE

        In each group with this algorithm associated, there can be only one
        highlander and only one blessed component. Only components that are
        ready (`ready` flag of `blessing_res` is ``True``) are considered as
        candidates for receiving blessing. In case there is no any ready
        component, this algorithm will not give blessing to any component.

        For determining which component receives the blessing, multiple ordered
        criteria are applied sequentially until there is only one component
        left. If any of the criteria is satisfied by more than one component,
        the next criteria is applied. Criteria are the following, respectively:

            1) the mathematically lowest `rank`
            2) request `token` previously set
            3) the lowest blessing `timestamp`
            4) the lowest `mid`

        Finally, when algorithm defined the component to be blessed, if it
        doesn't already have request token, `master` revokes previously issued
        request token from other component in the same group and waits until
        all components in the same group have revoked theirs response tokens.
        Only once all other components revoke their response tokens, master
        issues new request token to chosen component and sets new associated
        timestamp value.


Components rank
---------------

Association of component's `rank` is responsibility of component's local Monitor
Server for all of its local components. Monitor Server should associate same
rank as was last rank value associated with previously active client connection
with same `name` and `group` values as newly established connection. If such
previously active connection does not exist, default rank value, as specified
by Monitor Server's configuration, is applied. After initial rank value is
associated with client and its `ComponentInfo`, local Monitor Servers can
later change rank's value. These changes should be cached by local Monitor
Servers in case connection to component is lost and same component tries to
establish new connection. This cache is maintained for duration of single
Monitor Server process execution and is not persisted between different Monitor
Server processes.


User interface
--------------

As secondary functionality, Monitor Server provides web-based user interface
for monitoring global components state and controlling component's rank.
Implementation of this functionality is split into server-side web backend and
web frontend exchanging communication messages based on juggler communication
protocol.


Backend to frontend communication
'''''''''''''''''''''''''''''''''

Backend provides to frontends all information that is made available by server
to clients. When this information changes, all frontends are notified of
this change. Current state of `mid` and all components is set and continuously
updated as part of server's juggler local data.

After new Juggler connection is established, backend will immediately set
juggler local data defined by JSON schema
``hat-monitor://juggler.yaml#/definitions/data/server``.

Server doesn't send additional `MESSAGE` juggler messages.


Frontend to backend communication
'''''''''''''''''''''''''''''''''

This communication is used primary for enabling user control of components'
ranks. At any time, frontend can send `set_rank` message to backend requesting
change of rank for any available local component. This massage is
transmitted as juggler's `MESSAGE` messages defined by JSON schema
``hat-monitor://juggler.yaml#/definitions/messages/client/set_rank``.

Client's juggler local data isn't changed during communication with server (it
remains `null`).


Future features
---------------

.. todo::

    * optional connection to monitor/event server

        * mapping of current status to events
        * listening for control events


Implementation
--------------

Documentation is available as part of generated API reference:

    * `Python hat.monitor module <py_api/hat/monitor.html>`_


Chatter messages
----------------

.. literalinclude:: ../schemas_sbs/monitor.sbs
    :language: none


Juggler messages and data
-------------------------

.. literalinclude:: ../schemas_json/juggler.yaml
    :language: yaml


Configuration
-------------

.. literalinclude:: ../schemas_json/main.yaml
    :language: yaml


.. |arr| unicode:: U+003E
