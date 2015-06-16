(*
   Generic pool of reusable connections.
   See mli file.
*)

open Printf
open Lwt
open Log

module type Connection = sig
  type conn
  val create_connection : unit -> conn Lwt.t
  val close_connection : conn -> unit Lwt.t
  val is_reusable : conn -> bool Lwt.t
end

module type S = sig
  type connection_pool
  type conn
  val create_pool : capacity:int -> max_live_conn:int -> connection_pool
  val with_connection : connection_pool -> (conn -> 'a Lwt.t) -> 'a Lwt.t
end

module Make (C : Connection) = struct
  type conn = C.conn

  type connection_pool = {
    (* Queue of reusable connections *)
    queue : (conn * bool ref) Queue.t;
      (* the boolean indicates whether the connection is live,
         consistently with the live connection counter
         (and not necessarily with the actual state of the connection
         because of possibility of exceptions and double-closing). *)
    queue_capacity : int;

    (* System for limiting the number of simultaneous connections *)
    max_live_conn : int;
      (* maximum number of live (= open) connections *)
    live_conn : int ref;
      (* counter of live connections *)
    conn_possible : unit Lwt_condition.t;
      (* used to wake up a thread when a new connection can be created *)
  }

  let create_pool ~capacity ~max_live_conn =
    if capacity > max_live_conn then
      invalid_arg "Pool.Make().create_pool: \
                     pool capacity may not be greater than the maximum number \
                     of simultaneous connections"
    else {
      queue = Queue.create ();
      queue_capacity = capacity;
      max_live_conn;
      live_conn = ref 0;
      conn_possible = Lwt_condition.create ();
    }

  let create_connection p =
    C.create_connection () >>= fun conn ->
    incr p.live_conn;
    return (conn, ref true)

  let rec get_connection p =
    try return (Queue.take p.queue)
    with Queue.Empty ->
      if !(p.live_conn) >= p.max_live_conn then
        Lwt_condition.wait p.conn_possible >>= fun () ->
        get_connection p
      else
        create_connection p

  let close_connection p (connection, is_live) =
    Lwt.finalize
      (fun () -> C.close_connection connection)
      (fun () ->
         if !is_live then (
           (* avoid double counting *)
           decr p.live_conn;
           is_live := false;
         );
         assert (!(p.live_conn) >= 0);
         if !(p.live_conn) < p.max_live_conn then (
           (* Wake up one of the waiting threads *)
           Lwt_condition.signal p.conn_possible ()
         );
         return ()
      )

  let recycle_connection p connection =
    Queue.add connection p.queue;
    Lwt_condition.signal p.conn_possible ()

  let with_connection p f =
    get_connection p >>= fun ((conn, is_live) as connection) ->

    let save_or_close_connection () =
      C.is_reusable conn >>= function
      | false -> close_connection p connection
      | true ->
          if Queue.length p.queue < p.queue_capacity then (
            recycle_connection p connection;
            return ()
          )
          else
            close_connection p connection
    in

    catch
      (fun () ->
        f conn >>= fun result ->
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
        type conn = int ref * bool ref
        let create_connection () = return (ref 0, ref true)
        let close_connection _ = return ()
        let is_reusable (_, reusable) = return !reusable
      end
    in
    let module Connection_pool = Make(Connection) in
    let pool = Connection_pool.create_pool ~capacity:3 ~max_live_conn:5 in
    let t =
      let use_connection () =
        Connection_pool.with_connection pool (fun (count, _) ->
          incr count;
          Lwt_unix.sleep 0.25 >>= fun () ->
          return count
        )
      in
      let ignore_count = use_connection () >>= fun _ -> return () in
      join [ignore_count; ignore_count; ignore_count] >>= fun () ->
      (* That should hit the connection capacity, so now we're reusing a
         connection *)
      use_connection () >>= fun count ->
      assert (!count = 2);

      let break_connection () =
        Connection_pool.with_connection pool (fun (_, reusable) ->
          reusable := false;
          Lwt_unix.sleep 0.25 >>= fun () ->
          return ()
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

  (* Goals of this test:
     - make sure it terminates
     - make sure a close_connection that raises an exception doesn't
       bring the connection counter to a negative value
  *)
  let test_broken_connection_pools () =
    let module Connection =
      struct
        type conn = unit
        let create_connection () = return ()
        let close_connection () = raise Exit
        let is_reusable () = return true
      end
    in
    let module Connection_pool = Make(Connection) in
    let pool = Connection_pool.create_pool ~capacity:1 ~max_live_conn:3 in
    let job =
      (* In debug mode (Log.level := `Debug), this prints something like
         +++--++--++--++--++----
      *)
      Lwt_list.iter_p (fun () ->
        catch
          (fun () ->
             Connection_pool.with_connection pool (fun () ->
               if !Log.level = `Debug then printf "+%!";
               Lwt_unix.sleep 0.05 >>= fun () ->
               if !Log.level = `Debug then printf "-%!";
               return ()
             )
          )
          (function Exit -> return ()
                  | e -> raise e)
    ) [ (); (); (); (); (); (); (); (); (); (); (); ]
    in
    Lwt_main.run job;

    (* Check that we don't hang when max_live_conn = capacity *)
    let pool2 = Connection_pool.create_pool ~capacity:1 ~max_live_conn:1 in
    let job2 =
      Lwt_list.iter_p (fun () ->
        catch
          (fun () ->
             Connection_pool.with_connection pool2 (fun () ->
               Lwt_unix.sleep 0.05
             )
          )
          (function Exit -> return ()
                  | e -> raise e)
      ) [ (); (); (); (); ]
    in
    Lwt_main.run job2;

    true

  let tests = [
    "test_connection_pool", test_connection_pool;
    "test_broken_connection_pools", test_broken_connection_pools;
  ]
end

let tests = Test.tests
