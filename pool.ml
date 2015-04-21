open Lwt

module type Connection = sig
  type conn
  val create_connection : unit -> conn Lwt.t
  val close_connection : conn -> unit Lwt.t
  val is_reusable : conn -> bool Lwt.t
end

module type S = sig
  type connection_pool
  type conn
  val create_pool : capacity:int -> connection_pool
  val with_connection : connection_pool -> (conn -> 'a Lwt.t) -> 'a Lwt.t
end

module Make (C : Connection) = struct
  type conn = C.conn
  type connection_pool = conn Lwt_sequence.t * int

  let create_pool ~capacity = (Lwt_sequence.create (), capacity)

  let with_connection (pool, capacity) f =
    (match Lwt_sequence.take_opt_l pool with
    | None -> C.create_connection ()
    | Some c -> return c
    ) >>= fun connection ->

    let save_or_close_connection () =
      C.is_reusable connection >>= function
      | false -> C.close_connection connection
      | true ->
          if Lwt_sequence.length pool < capacity then
            let _ = Lwt_sequence.add_r connection pool in
            return ()
          else C.close_connection connection
    in

    catch
      (fun () ->
        f connection >>= fun result ->
        save_or_close_connection () >>= fun () ->
        return result
      )
      (fun e ->
        save_or_close_connection () >>= fun () ->
        fail e
      )
end
