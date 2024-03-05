package io.dwsoft.sok.coroutines

import io.dwsoft.sok.Saga
import io.dwsoft.sok.saga
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlin.coroutines.EmptyCoroutineContext

@OptIn(ExperimentalCoroutinesApi::class)
class SuspendingSagaTest : FreeSpec({
    "test" {
        val scheduler = TestCoroutineScheduler()
        val sagaCoContext = CoroutineScope(EmptyCoroutineContext + StandardTestDispatcher(scheduler))
        val saga = with(sagaCoContext) {
            saga<Job, Nothing> {
                result { this@with.launch { delay(1000) } }
            }
        }

        val result = saga.execute().shouldBeInstanceOf<Saga.Result.Success<Job>>().value

        scheduler.advanceTimeBy(500)
        result.isActive shouldBe true
        result.isCancelled shouldBe false
        sagaCoContext.cancel("lol")
        result.isActive shouldBe false
        result.isCancelled shouldBe true
    }
})
