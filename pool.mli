(* Pools of reusable connections, growing to a configurable capacity *)

module type Connection = sig
  type conn
  val create_connection : unit -> conn Lwt.t
  val close_connection : conn -> unit Lwt.t
  val is_reusable : conn -> bool Lwt.t
end

module type S = sig
  type conn
  type connection_pool
  val create_pool : capacity:int -> max_live_conn:int -> connection_pool
  val with_connection : connection_pool -> (conn -> 'a Lwt.t) -> 'a Lwt.t
end

module Make : functor (C : Connection) -> S with type conn = C.conn

val tests : (string * (unit -> bool)) list
