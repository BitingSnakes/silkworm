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

structure RetryConfig where
  maxTimes : Nat := 3
  retryStatuses : List Nat := [500, 502, 503, 504, 522, 524, 408, 429]
  sleepStatuses : List Nat := [500, 502, 503, 504, 522, 524, 408, 429]
deriving Repr, DecidableEq

def shouldRetryStatus (cfg : RetryConfig) (status : Nat) : Prop :=
  status ∈ cfg.retryStatuses ∨ status ∈ cfg.sleepStatuses

instance (cfg : RetryConfig) (status : Nat) : Decidable (shouldRetryStatus cfg status) := by
  unfold shouldRetryStatus
  infer_instance

inductive ResponseMwResult where
  | keep (resp : Response)
  | retry (req : Request)
deriving Repr, DecidableEq

def retryDecision
    (cfg : RetryConfig)
    (response : Response) : ResponseMwResult :=
  if shouldRetryStatus cfg response.status then
    if response.request.retryTimes >= cfg.maxTimes then
      ResponseMwResult.keep response
    else
      ResponseMwResult.retry (retryRequest response.request)
  else
    ResponseMwResult.keep response

theorem retryDecision_non_retry_status_keeps
    (cfg : RetryConfig)
    (response : Response)
    (hStatus : ¬ shouldRetryStatus cfg response.status) :
    retryDecision cfg response = ResponseMwResult.keep response := by
  simp [retryDecision, hStatus]

theorem retryDecision_at_max_keeps
    (cfg : RetryConfig)
    (response : Response)
    (hStatus : shouldRetryStatus cfg response.status)
    (hMax : response.request.retryTimes >= cfg.maxTimes) :
    retryDecision cfg response = ResponseMwResult.keep response := by
  simp [retryDecision, hStatus, hMax]

theorem retryDecision_below_max_retries
    (cfg : RetryConfig)
    (response : Response)
    (hStatus : shouldRetryStatus cfg response.status)
    (hBelow : response.request.retryTimes < cfg.maxTimes) :
    retryDecision cfg response = ResponseMwResult.retry (retryRequest response.request) := by
  have hNotMax : ¬ response.request.retryTimes >= cfg.maxTimes := Nat.not_le_of_gt hBelow
  simp [retryDecision, hStatus, hNotMax]

def applyResponseMwResult
    (result : ResponseMwResult)
    (st : EngineState) : EngineState :=
  match result with
  | ResponseMwResult.keep _ => st
  | ResponseMwResult.retry req => enqueue req st

def applyResponseMwResultWith
    (key : DedupKey)
    (result : ResponseMwResult)
    (st : EngineState) : EngineState :=
  match result with
  | ResponseMwResult.keep _ => st
  | ResponseMwResult.retry req => enqueueWith key req st

theorem applyResponseMwResultWith_preserves_prioritySorted
    (key : DedupKey)
    (result : ResponseMwResult)
    (st : EngineState)
    (hSorted : PrioritySorted st.queue) :
    PrioritySorted (applyResponseMwResultWith key result st).queue := by
  cases result with
  | keep response =>
      simp [applyResponseMwResultWith, hSorted]
  | retry req =>
      simp [applyResponseMwResultWith]
      exact enqueueWith_preserves_prioritySorted key req st hSorted

end Silkworm
