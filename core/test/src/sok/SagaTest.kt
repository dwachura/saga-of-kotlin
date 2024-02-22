package io.dwsoft.sok

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.spyk
import io.mockk.verify
import java.util.concurrent.Executors

class SagaTest : FreeSpec({
    "Saga with successful actions finishes successfully" {
        val result = saga<Int, Nothing> {
            val v1 = act({ 1 }, orCompensate = {})
            val v2 = act({ 2 }, orCompensate = {})
            act({ v1.value + v2.value }, orCompensate = {})
        }.execute()

        result.shouldBeInstanceOf<Saga.Result.Completed<Int>>()
            .value shouldBe 3
    }

    "Saga with unsuccessful action finishes unsuccessfully with failure reason" {
        val saga = saga<Unit, Exception> {
            act({ fail(RuntimeException("Test")) }, orCompensate = {})
        }

        val result = saga.execute()

        result.shouldBeInstanceOf<Saga.Result.Compensated<Exception>>()
            .reason.shouldBeInstanceOf<RuntimeException>()
            .shouldHaveMessage("Test")
    }

    "Saga with unsuccessful action reverts actions completed successfully" {
        val rollbacks = listOf<() -> Unit>(spyk(), spyk(), spyk())
        val saga = saga<Unit, Exception> {
            act({}, orCompensate = rollbacks[0])
            act({}, orCompensate = rollbacks[1])
            act({ fail(RuntimeException()) }, orCompensate = rollbacks[2])
        }

        runCatching { saga.execute() }

        verify(exactly = 1) { rollbacks[0]() }
        verify(exactly = 1) { rollbacks[1]() }
        verify(exactly = 0) { rollbacks[2]() }
    }

    "Saga with action which rollback fails fails fast with exception" {
        val rollback = spyk<() -> Unit>()
        val saga = saga<Unit, Unit> {
            act({}, orCompensate = { throw RuntimeException("Test") })
            act({}, orCompensate = rollback)
            act({ fail(Unit) }, orCompensate = rollback)
        }

        shouldThrow<SagaException> {
            saga.execute()
        }.cause.shouldBeInstanceOf<RuntimeException>()
            .shouldHaveMessage("Test")
    }

    "Sample: Parallel ops" {
        val result = saga<String, Nothing> {
            val pool = Executors.newFixedThreadPool(2)
            val s1 = act({
                pool.submit<String> {
                    println("Step 1 started")
                    Thread.sleep(1000)
                    "Hello".also { println("Step 1 finished") }
                }
            }, orCompensate = {})
            val result = act({
                println("Step 2 started")
                val v = s1.value.get()
                "$v world".also { println("Step 2 finished") }
            }, orCompensate = {})
            act({ result.value.also(::println) }, orCompensate = {})
        }.execute()

        result.shouldBeInstanceOf<Saga.Result.Completed<String>>()
            .value shouldBe "Hello world"
    }
})
