(*
   Pools of reusable connections, growing to a configurable capacity

   - taking a connection from the pool consists in taking a connection from
     a queue of connections available for reuse; if the queue is empty,
     a new connection is created
   - when done with a connection, it is added back to the queue; if the
     queue is full, the connection is closed and not added to the queue.
   - no more than the configurable max_live_conn connections exist
     simultaneously
*)

module type Connection = sig
  type conn
  val create_connection : unit -> conn Lwt.t
  val close_connection : conn -> unit Lwt.t
  val is_reusable : conn -> bool Lwt.t
end

module type S = sig
  type conn
  type connection_pool
  val create_pool :
    capacity:int ->
    max_live_conn:int -> connection_pool
    (* capacity: maximum number of unused connections to keep around
                 for later reuse
       max_live_conn:
                 maximum number of connections existing simultaneously.

       max_live_conn must be greater than or equal to capacity.
    *)

  val with_connection : connection_pool -> (conn -> 'a Lwt.t) -> 'a Lwt.t
    (* Run an arbitrary function with connection reused from the pool
       or freshly created. This obeys max_live_conn by waiting
       until the number of simultaneous connections drops below max_live_conn.
    *)
end

module Make : functor (C : Connection) -> S with type conn = C.conn

val tests : (string * (unit -> bool)) list
