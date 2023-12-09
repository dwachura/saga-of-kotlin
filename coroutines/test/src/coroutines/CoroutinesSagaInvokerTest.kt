package io.dwsoft.sok

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowMessage
import io.kotest.core.spec.style.FreeSpec
import io.kotest.core.test.testCoroutineScheduler
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalStdlibApi::class, ExperimentalCoroutinesApi::class)
class CoroutinesSagaInvokerTest : FreeSpec({
    "saga is processed sequentially and returns correct result".config(coroutineTestScope = true) {
        val sagaScope = CoroutineScope(coroutineContext)
        val expectedObj = mutableListOf<Int>()
        fun delayedPhase(delaySeconds: Int) =
            ReversibleOperation(
                { delay(delaySeconds.seconds); expectedObj += delaySeconds }
            ) {}
        val saga = Saga(
            listOf(delayedPhase(3), delayedPhase(2), delayedPhase(1))
        ) { expectedObj }

        val result = async { CoroutinesSagaProcessor().runner.execute(saga) }

        testCoroutineScheduler.runCurrent()
        result.isCompleted shouldBe false
        expectedObj.shouldBeEmpty()
        testCoroutineScheduler.advanceBy(3.seconds)
        expectedObj.shouldContainExactly(3)
        testCoroutineScheduler.advanceBy(2.seconds)
        expectedObj.shouldContainExactly(3, 2)
        testCoroutineScheduler.advanceBy(1.seconds)
        result.isCompleted shouldBe true
        expectedObj.shouldContainExactly(3, 2, 1)
        result.await() shouldBe expectedObj
    }

    "saga is processed concurrently and returns correct result".config(coroutineTestScope = true) {
        val expectedObj = mutableListOf<Int>()
        fun delayedPhase(delaySeconds: Int): Pair<SagaPhase, Lazy<Job>> {
            var job: Job? = null
            val phase = ReversibleOperation(
                {
                    job = launch {
                        delay(delaySeconds.seconds)
                        expectedObj += delaySeconds
                    }
                }
            ) {}
            return phase to lazy { requireNotNull(job) }
        }
        val (phases, jobs) = (3 downTo 1).associate { delayedPhase(it) }
            .let { it.keys.toList() to it.values.toList() }
        val saga = Saga(phases) {
            jobs.forEach { it.value.join() }
            expectedObj
        }

        val result = async { CoroutinesSagaProcessor().runner.execute(saga) }

        testCoroutineScheduler.runCurrent()
        result.isCompleted shouldBe false
        testCoroutineScheduler.advanceBy(3.seconds)
        result.isCompleted shouldBe true
        expectedObj.shouldContainExactly(1, 2, 3)
        result.await() shouldBe expectedObj
    }

    "failed step is not reverted" {
        var obj: Any? = null
        val phase = ReversibleOperation({ obj = Any(); error("fail") }) { obj = null }
        val saga = Saga(listOf(phase))

        shouldThrowMessage("fail") {
            CoroutinesSagaProcessor().runner.execute(saga)
        }
        obj shouldNotBe null
    }

    "steps defined after failed one are not executed" {
        var obj: Any? = Any()
        val saga = Saga(
            listOf(
                ReversibleOperation({ error("fail") }) {},
                ReversibleOperation({ obj = null }) {}
            )
        )

        shouldThrowMessage("fail") {
            CoroutinesSagaProcessor().runner.execute(saga)
        }
        obj shouldNotBe null
    }

    "every completed step is reverted" {
        val map = mutableMapOf<String, Int>()
        val successfulPhases = (1..2).map { idx ->
            ReversibleOperation(
                { map["phase$idx"] = idx }
            ) { map.remove("phase$idx") }
        }
        val failedStep = ReversibleOperation({ error("fail") }) {}
        val saga = Saga(successfulPhases + failedStep)

        shouldThrowMessage("fail") {
            CoroutinesSagaProcessor().runner.execute(saga)
        }
        map.shouldBeEmpty()
    }

    "fatal error doesn't trigger revert process" {
        val fakeError = object : Error() {}
        var obj: Any? = null
        val saga = Saga(listOf(
            ReversibleOperation({ obj = Any() }) { obj = null },
            ReversibleOperation({ throw fakeError  }) {},
        ))

        shouldThrow<Error> {
            CoroutinesSagaProcessor().runner.execute(saga)
        } shouldBe fakeError
        obj shouldNotBe null
    }
})

/**
 * Shortcut for [advanceTimeBy(duration)][TestCoroutineScheduler.advanceTimeBy] followed
 * by [runCurrent()][TestCoroutineScheduler.runCurrent].
 */
private fun TestCoroutineScheduler.advanceBy(duration: Duration) {
    advanceTimeBy(duration)
    runCurrent()
}
