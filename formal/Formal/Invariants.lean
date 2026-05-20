import Formal.Callback
import Formal.Engine

namespace Silkworm

theorem handle_no_callback_output_noop (st : EngineState) :
    handleCallbackOutput CallbackOutput.none st = st := by
  simp [handleCallbackOutput, normalizeCallbackOutput, handleEvents]

end Silkworm
