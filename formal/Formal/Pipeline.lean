import Formal.Request

namespace Silkworm

/--
  CallbackPipeline semantics: `none` means "keep the original item", not
  "drop the item".
-/
abbrev Pipeline := Item -> Option Item

def applyPipeline (pipeline : Pipeline) (item : Item) : Item :=
  match pipeline item with
  | some item' => item'
  | none => item

def processPipelines (pipes : List Pipeline) (item : Item) : Item :=
  pipes.foldl (fun acc pipeline => applyPipeline pipeline acc) item

theorem processPipelines_nil (item : Item) :
    processPipelines [] item = item := by
  simp [processPipelines]

theorem processPipelines_append
    (xs ys : List Pipeline)
    (item : Item) :
    processPipelines (xs ++ ys) item =
      processPipelines ys (processPipelines xs item) := by
  simp [processPipelines, List.foldl_append]

end Silkworm
