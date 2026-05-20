import Cslib.Foundations.Semantics.LTS.Basic
import Cslib.Foundations.Semantics.LTS.Execution
import Formal.Retry

namespace Silkworm

inductive EngineLabel where
  | enqueue (req : Request)
  | requestSent (req : Request)
  | responseReceived (response : Response)
  | unhandledError (req : Request)
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
  | requestSent :
      EngineStep key st (.requestSent req) (requestSent req st)
  | responseReceived :
      EngineStep key st (.responseReceived response) (responseReceived response st)
  | unhandledError :
      EngineStep key st (.unhandledError req) (unhandledError req st)
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
      · exact ih (handleEventWith key event st)

theorem callbackOutput_mtr
    (key : DedupKey)
    (out : CallbackOutput)
    (st : EngineState) :
    (engineLTS key).MTr
      st
      (callbackEventLabels (normalizeCallbackOutput out))
      (handleCallbackOutputWith key out st) := by
  simpa [handleCallbackOutputWith] using
    callbackEvents_mtr key (normalizeCallbackOutput out) st

theorem callbackEvents_default_mtr
    (events : List Event)
    (st : EngineState) :
    (engineLTS defaultDedupKey).MTr
      st
      (callbackEventLabels events)
      (handleEvents events st) := by
  simpa [handleEvents] using callbackEvents_mtr defaultDedupKey events st

theorem callbackOutput_default_mtr
    (out : CallbackOutput)
    (st : EngineState) :
    (engineLTS defaultDedupKey).MTr
      st
      (callbackEventLabels (normalizeCallbackOutput out))
      (handleCallbackOutput out st) := by
  simpa [handleCallbackOutput] using callbackOutput_mtr defaultDedupKey out st

theorem callbackEvents_execution
    (key : DedupKey)
    (events : List Event)
    (st : EngineState) :
    ∃ states : List EngineState,
      (engineLTS key).Execution
        st
        (callbackEventLabels events)
        (handleEventsWith key events st)
        states :=
  Cslib.LTS.Execution.of_mTr (callbackEvents_mtr key events st)

theorem engineStep_preserves_prioritySorted
    {key : DedupKey}
    {st st' : EngineState}
    {label : EngineLabel}
    (hStep : EngineStep key st label st') :
    PrioritySorted st.queue ->
    PrioritySorted st'.queue := by
  intro hSorted
  cases hStep with
  | enqueue =>
      exact enqueueWith_preserves_prioritySorted key _ _ hSorted
  | requestSent =>
      simpa [requestSent] using hSorted
  | responseReceived =>
      simpa [responseReceived] using hSorted
  | unhandledError =>
      simpa [unhandledError] using hSorted
  | callbackEvent =>
      exact handleEventWith_preserves_prioritySorted key _ _ hSorted
  | retry =>
      exact enqueueWith_preserves_prioritySorted key _ _ hSorted
  | scrape =>
      simpa [scrapeItem] using hSorted

theorem engineMTr_preserves_prioritySorted
    {key : DedupKey}
    {st st' : EngineState}
    {labels : List EngineLabel}
    (hTrace : (engineLTS key).MTr st labels st') :
    PrioritySorted st.queue ->
    PrioritySorted st'.queue := by
  intro hSorted
  induction hTrace with
  | refl =>
      exact hSorted
  | stepL hStep _ ih =>
      exact ih (engineStep_preserves_prioritySorted hStep hSorted)

theorem callbackEvents_preserve_prioritySorted
    (key : DedupKey)
    (events : List Event)
    (st : EngineState)
    (hSorted : PrioritySorted st.queue) :
    PrioritySorted (handleEventsWith key events st).queue :=
  engineMTr_preserves_prioritySorted (callbackEvents_mtr key events st) hSorted

theorem callbackOutput_preserve_prioritySorted
    (key : DedupKey)
    (out : CallbackOutput)
    (st : EngineState)
    (hSorted : PrioritySorted st.queue) :
    PrioritySorted (handleCallbackOutputWith key out st).queue :=
  engineMTr_preserves_prioritySorted (callbackOutput_mtr key out st) hSorted

end Silkworm
