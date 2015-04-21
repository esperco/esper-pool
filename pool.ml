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

module Test = struct
  let test_connection_pool () =
    let module Connection =
      struct
        type conn = int ref * Lwt_mutex.t * bool ref
        let create_connection () =
          return (ref 0, Lwt_mutex.create (), ref true)
        let close_connection _ =
          return ()
        let is_reusable (_, _, reusable) =
          return !reusable
      end
    in
    let module Connection_pool = Make(Connection) in
    let pool = Connection_pool.create_pool ~capacity:3 in
    let t =
      let use_connection () =
        Connection_pool.with_connection pool (fun (count, mutex, _) ->
          Lwt_mutex.with_lock mutex (fun () ->
            incr count;
            Lwt_unix.sleep 0.25 >>= fun () ->
            return count
          )
        )
      in
      let ignore_count = use_connection () >>= fun _ -> return () in
      join [ignore_count; ignore_count; ignore_count] >>= fun () ->
      (* That should hit the connection capacity, so now we're reusing a
         connection *)
      use_connection () >>= fun count ->
      assert (!count = 2);

      let break_connection () =
        Connection_pool.with_connection pool (fun (_, mutex, reusable) ->
          Lwt_mutex.with_lock mutex (fun () ->
            reusable := false;
            Lwt_unix.sleep 0.25 >>= fun () ->
            return ()
          )
        )
      in
      join [break_connection (); break_connection (); break_connection ()]
      >>= fun () ->
      (* All those non-reusable connections should be cleared out, so if we
         use one now, it'll be a new connection *)
      use_connection () >>= fun count ->
      return (!count = 1)
    in
    Util_lwt_main.run t

  let tests = [
    ("test_connection_pool", test_connection_pool)
  ]
end

let tests = Test.tests
