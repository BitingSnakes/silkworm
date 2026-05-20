import Formal.Engine

namespace Silkworm

def retryRequest (req : Request) : Request :=
  { req with
    dontFilter := true
    retryTimes := req.retryTimes + 1 }

theorem retry_bypasses_dedup (req : Request) :
    (retryRequest req).dontFilter = true := by
  simp [retryRequest]

theorem retry_increments_retryTimes (req : Request) :
    (retryRequest req).retryTimes = req.retryTimes + 1 := by
  simp [retryRequest]

inductive ResponseMwResult where
  | keep (resp : Response)
  | retry (req : Request)
deriving Repr, DecidableEq

def applyResponseMwResult
    (result : ResponseMwResult)
    (st : EngineState) : EngineState :=
  match result with
  | ResponseMwResult.keep _ => st
  | ResponseMwResult.retry req => enqueue req st

end Silkworm
