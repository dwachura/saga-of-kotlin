package io.dwsoft.sok

import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.shouldNotBe

class SagaTest : FreeSpec({
    "failed step is not reverted" {
        var obj: Any? = null
        val revertible = Revertible({ obj = Any(); error("fail") }) { obj = null }
        val saga = Saga(steps = listOf(revertible)) {}

        shouldThrowMessage("fail") {
            saga.invoke()
        }
        obj shouldNotBe null
    }

    "every completed step is reverted" {
        val map = mutableMapOf<String, Int>()
        val successfulSteps = (1..2).map { idx ->
            Revertible(
                { map["phase$idx"] = idx }
            ) { map.remove("phase$idx") }
        }
        val failedStep = Revertible({ error("fail") }) {}
        val saga = Saga(steps = successfulSteps + failedStep) {}

        shouldThrowMessage("fail") {
            saga.invoke()
        }
        map.shouldBeEmpty()
    }
})
