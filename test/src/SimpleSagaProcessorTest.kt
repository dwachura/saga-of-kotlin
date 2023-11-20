package io.dwsoft.sok

import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe

class SimpleSagaProcessorTest : FreeSpec({
    "competed saga returns correct result" {
        val expectedObj = Any()
        val saga = Saga(emptyList()) { expectedObj }

        val result = SimpleSagaElementProcessor().execute(saga)

        result shouldBe expectedObj
    }

    "failed step is not reverted" {
        var obj: Any? = null
        val phase = SagaPhase({ obj = Any(); error("fail") }) { obj = null }
        val saga = Saga(listOf(phase))

        shouldThrowMessage("fail") {
            SimpleSagaElementProcessor().execute(saga)
        }
        obj shouldNotBe null
    }

    "steps defined after failed one are not executed" {
        var obj: Any? = Any()
        val saga = Saga(
            listOf(
                SagaPhase({ error("fail") }) {},
                SagaPhase({ obj = null }) {}
            )
        )

        shouldThrowMessage("fail") {
            SimpleSagaElementProcessor().execute(saga)
        }
        obj shouldNotBe null
    }

    "every completed step is reverted" {
        val map = mutableMapOf<String, Int>()
        val successfulPhases = (1..2).map { idx ->
            SagaPhase(
                { map["phase$idx"] = idx }
            ) { map.remove("phase$idx") }
        }
        val failedStep = SagaPhase({ error("fail") }) {}
        val saga = Saga(successfulPhases + failedStep)

        shouldThrowMessage("fail") {
            SimpleSagaElementProcessor().execute(saga)
        }
        map.shouldBeEmpty()
    }
})
