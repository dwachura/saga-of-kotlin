package io.dwsoft.sok

import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldNotBe

class SagaTest : FreeSpec({
    "failed phase is not compensated" {
        var obj: Any? = null
        val saga = saga {
            phase { obj = Any(); error("fail") } compensate { obj = null }
        }

        shouldThrowMessage("fail") {
            saga.exec()
        }
        obj shouldNotBe null
    }

    "all completed phases are compensated" {
        val map = mutableMapOf<String, Int>()
        saga {
            phase { map["phase1"] = 1 } compensate { map.remove("phase1") }
            phase { map["phase1"] = 1 } compensate { map.remove("phase1") }
        }
    }

})
