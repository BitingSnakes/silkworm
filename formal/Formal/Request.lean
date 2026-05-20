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

inductive CallbackOutput where
  | none
  | one (event : Event)
  | many (events : List Event)
deriving Repr, DecidableEq

def normalizeCallbackOutput : CallbackOutput -> List Event
  | CallbackOutput.none => []
  | CallbackOutput.one event => [event]
  | CallbackOutput.many events => events

end Silkworm
