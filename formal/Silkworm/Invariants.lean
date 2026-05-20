import Silkworm.Callback
import Silkworm.Engine

namespace Silkworm

theorem handle_no_callback_output_noop (st : EngineState) :
    handleCallbackOutput CallbackOutput.none st = st := by
  simp [
    handleCallbackOutput,
    handleCallbackOutputWith,
    normalizeCallbackOutput,
    handleEventsWith,
  ]

end Silkworm
