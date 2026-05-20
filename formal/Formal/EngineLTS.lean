import Cslib.Foundations.Semantics.LTS.Basic
import Cslib.Foundations.Semantics.LTS.Execution
import Formal.Retry

namespace Silkworm

inductive EngineLabel where
  | enqueue (req : Request)
  | callbackEvent (event : Event)
  | retry (req : Request)
  | scrape (item : Item)
deriving Repr, DecidableEq

/--
  Labelled transition relation for the abstract Silkworm engine model.

  The relation is parameterized by the same deduplication key used by
  `enqueueWith`, matching the configurable `Engine(dedup_key=...)` contract in
  the Python implementation.
-/
inductive EngineStep (key : DedupKey) : EngineState -> EngineLabel -> EngineState -> Prop where
  | enqueue :
      EngineStep key st (.enqueue req) (enqueueWith key req st)
  | callbackEvent :
      EngineStep key st (.callbackEvent event) (handleEventWith key event st)
  | retry :
      EngineStep key st (.retry req) (enqueueWith key (retryRequest req) st)
  | scrape :
      EngineStep key st (.scrape item) (scrapeItem item st)

def engineLTS (key : DedupKey) : Cslib.LTS EngineState EngineLabel where
  Tr := EngineStep key

def callbackEventLabels (events : List Event) : List EngineLabel :=
  events.map EngineLabel.callbackEvent

theorem callbackEvents_mtr
    (key : DedupKey)
    (events : List Event)
    (st : EngineState) :
    (engineLTS key).MTr
      st
      (callbackEventLabels events)
      (handleEventsWith key events st) := by
  induction events generalizing st with
  | nil =>
      simp [callbackEventLabels, handleEventsWith]
      exact Cslib.LTS.MTr.refl
  | cons event events ih =>
      simp [callbackEventLabels, handleEventsWith]
      apply Cslib.LTS.MTr.stepL
      · exact EngineStep.callbackEvent
      · exact ih key events (handleEventWith key event st)

theorem callbackOutput_mtr
    (key : DedupKey)
    (out : CallbackOutput)
    (st : EngineState) :
    (engineLTS key).MTr
      st
      (callbackEventLabels (normalizeCallbackOutput out))
      (handleEventsWith key (normalizeCallbackOutput out) st) :=
  callbackEvents_mtr key (normalizeCallbackOutput out) st

theorem callbackEvents_default_mtr
    (events : List Event)
    (st : EngineState) :
    (engineLTS defaultDedupKey).MTr
      st
      (callbackEventLabels events)
      (handleEvents events st) := by
  simpa [handleEvents] using callbackEvents_mtr defaultDedupKey events st

end Silkworm
