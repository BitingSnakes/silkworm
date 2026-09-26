namespace Silkworm

abbrev Url := String
abbrev Item := String

inductive Method where
  | GET
  | HEAD
  | POST
  | PUT
  | PATCH
  | DELETE
  | OPTIONS
  | OTHER (name : String)
deriving Repr, BEq, DecidableEq

structure Request where
  url : Url
  method : Method := Method.GET
  hasBody : Bool := false
  hasJson : Bool := false
  hasParams : Bool := false
  dontFilter : Bool := false
  priority : Int := 0
  /-- Abstracts Python `Request.meta["retry_times"]` when present. -/
  retryTimes : Nat := 0
  /-- Abstracts Python `Request.meta["redirect_times"]` when present. -/
  redirects : Nat := 0
deriving Repr, DecidableEq

structure Response where
  url : Url
  status : Nat
  isHtml : Bool
  request : Request
deriving Repr, DecidableEq

inductive Event where
  | request (req : Request)
  | item (item : Item)
deriving Repr, DecidableEq

/--
  Ordered trace of the `await spider.emit(item)` (`Event.item`) and
  `await spider.follow(request)` (`Event.request`) calls made by one callback,
  errback, or `start_requests()` run. Each effect is applied to the engine as
  soon as it is awaited, so a callback is modelled by the sequence of its effects.
-/
abbrev CallbackEffects := List Event

end Silkworm
