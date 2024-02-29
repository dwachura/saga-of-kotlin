package io.dwsoft.sok

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.core.spec.style.FreeSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveMessage
import io.kotest.matchers.types.shouldBeInstanceOf
import io.kotest.matchers.types.shouldBeTypeOf
import io.mockk.spyk
import io.mockk.verify
import java.util.concurrent.Executors

// TODO: maybe group test cases by execution and rollback phases
class SagaTest : FreeSpec({
    "Action results are usable in..." - {
        "...other actions" {
            val recorder: (Int) -> Unit = spyk()
            saga<Unit, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                atomic({ recorder(v.value) }, compensateWith = {})
                result {}
            }.execute()

            verify(exactly = 1) { recorder(1) }
        }

        "...rollbacks" {
            val recorder: (Int) -> Unit = spyk()
            saga {
                val v = atomic({ 1 }, compensateWith = {})
                atomic({ 2 }, compensateWith = {
                    recorder(v.value)
                    recorder(it)
                })
                result { fail(Unit) }
            }.execute()

            verify(exactly = 1) { recorder(1) }
            verify(exactly = 1) { recorder(2) }
        }

        "...completions" {
            val recorder: (Int) -> Unit = spyk()
            saga<Unit, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                result { recorder(v.value) }
            }.execute()

            verify(exactly = 1) { recorder(1) }
        }
    }

    "Saga compensates completed actions when..." - {
        "...action fails" {
            val expectedReason = Any()
            val expectedRollback = spyk<SagaActionRollbackScope.(Any) -> Unit>()
            val unexpectedRollback = spyk<SagaActionRollbackScope.(Any) -> Unit>()
            val saga = saga {
                atomic({ 1 }, compensateWith = expectedRollback)
                atomic({ 2 }, compensateWith = expectedRollback)
                atomic({ fail(expectedReason) }, compensateWith = unexpectedRollback)
                result {}
            }

            val result = saga.execute()

            result.shouldBeInstanceOf<Saga.Result.Compensated<*>>()
                .reason shouldBe expectedReason
            verify(exactly = 1) { expectedRollback(any(), 1) }
            verify(exactly = 1) { expectedRollback(any(), 2) }
            verify(exactly = 0) { unexpectedRollback(any(), any()) }
        }

        "...completion fails" {
            val expectedReason = Any()
            val rollback = spyk<SagaActionRollbackScope.(Any) -> Unit>()
            val saga = saga {
                atomic({ 1 }, compensateWith = rollback)
                atomic({ 2 }, compensateWith = rollback)
                result { fail(expectedReason) }
            }

            val result = saga.execute()

            result.shouldBeInstanceOf<Saga.Result.Compensated<*>>()
                .reason shouldBe expectedReason
            verify(exactly = 1) { rollback(any(), 1) }
            verify(exactly = 1) { rollback(any(), 2) }
        }

        "Rollbacks are called when action throws non-fatal exception" {
            shouldThrowAny { TODO("Is this behavior correct/needed???") }
        }

        "Rollbacks are called when completion throws non-fatal exception" {
            shouldThrowAny { TODO("Is this behavior correct/needed???") }
        }
    }

    "Saga with successful actions finishes successfully" {
        val result = saga<Int, Nothing> {
            val v1 = atomic({ 1 }, compensateWith = {})
            val v2 = atomic({ 2 }, compensateWith = {})
            result { v1.value + v2.value }
        }.execute()

        result.shouldBeInstanceOf<Saga.Result.Completed<Int>>()
            .value shouldBe 3
    }

    "Saga fails fast when exception is thrown from..." - {
        "...action" {
            val expectedException = RuntimeException()
            val recorder = spyk<SagaActionRollbackScope.(Unit) -> Unit>()
            val saga = saga<Unit, Unit> {
                atomic({}, compensateWith = recorder)
                atomic({ throw expectedException }, compensateWith = {})
                result {}
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedException
            verify(exactly = 0) { recorder(any(), Unit) }
        }

        "...rollback" {
            val expectedException = RuntimeException()
            val recorder = spyk<SagaActionRollbackScope.(Unit) -> Unit>()
            val saga = saga<Unit, Unit> {
                atomic({}, compensateWith = recorder)
                atomic({}, compensateWith = { throw expectedException })
                atomic({}, compensateWith = recorder)
                atomic({ fail(Unit) }, compensateWith = {})
                result {}
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedException
            verify(exactly = 1) { recorder(any(), Unit) }
        }

        "... completion" {
            val expectedException = RuntimeException()
            val recorder = spyk<SagaActionRollbackScope.(Unit) -> Unit>()
            val saga = saga<Unit, Unit> {
                atomic({}, compensateWith = recorder)
                atomic({}, compensateWith = { throw expectedException })
                atomic({}, compensateWith = recorder)
                result { fail(Unit) }
            }

            shouldThrow<SagaException> {
                saga.execute()
            }.cause shouldBe expectedException
            verify(exactly = 1) { recorder(any(), Unit) }
        }
    }

    "Saga nesting" - {
        "Successful nested saga returns value" {
            val saga = saga<Int, Nothing> {
                val v = atomic({ 1 }, compensateWith = {})
                result { v.value }
            }

            val result = saga<Int, Nothing> {
                val v = atomic(saga)
                result { v.value }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Completed<Int>>()
                .value shouldBe 1
        }

        "Nested saga is compensated" {
            val recorder = spyk<SagaActionRollbackScope.(Int) -> Unit>()
            val saga = saga<Int, Nothing> {
                val v1 = atomic({ 1 }, compensateWith = recorder)
                val v2 = atomic({ 2 }, compensateWith = recorder)
                result { v1.value + v2.value }
            }

            val result = saga<Int, Unit> {
                atomic(saga)
                result { fail(Unit) }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Compensated<Unit>>()
            verify(exactly = 1) { recorder(any(), 1) }
            verify(exactly = 1) { recorder(any(), 2) }
        }

        "Failed nested saga triggers compensation" {
            val recorder = spyk<SagaActionRollbackScope.(Int) -> Unit>()
            val saga = saga<Int, String> {
                val v1 = atomic({ 2 }, compensateWith = recorder)
                val v2 = atomic({ fail("Nested") }, compensateWith = recorder)
                result { v1.value + v2.value }
            }

            val result = saga<Int, String> {
                atomic({ 1 }, compensateWith = recorder)
                val v = atomic(saga)
                result { v.value }
            }.execute()

            result.shouldBeInstanceOf<Saga.Result.Compensated<String>>()
                .reason shouldBe "Nested"
            verify(exactly = 1) { recorder(any(), 1) }
            verify(exactly = 1) { recorder(any(), 2) }
            verify(exactly = 0) { recorder(any(), more(2)) }
        }

        "Saga fails fast when errors are thrown from nested saga's..." - {
            "...action" {
                val expectedException = RuntimeException()
                val recorder = spyk<SagaActionRollbackScope.(Int) -> Unit>()
                val nestedSaga = saga<Unit, Nothing> {
                    atomic({ 2 }, compensateWith = recorder)
                    atomic({ throw expectedException }, compensateWith = recorder)
                    atomic({ 3 }, compensateWith = recorder)
                    result {}
                }
                val saga = saga<Unit, String> {
                    atomic({ 1 }, compensateWith = recorder)
                    atomic(nestedSaga)
                    result {}
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }.cause shouldBe expectedException
                verify(exactly = 0) { recorder(any(), any()) }
            }

            "...completion" {
                val expectedException = RuntimeException()
                val recorder = spyk<SagaActionRollbackScope.(Int) -> Unit>()
                val nestedSaga = saga<Unit, Nothing> {
                    atomic({ 2 }, compensateWith = recorder)
                    result { throw expectedException }
                }
                val saga = saga<Unit, String> {
                    atomic({ 1 }, compensateWith = recorder)
                    atomic(nestedSaga)
                    result {}
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }.cause shouldBe expectedException
                verify(exactly = 0) { recorder(any(), any()) }
            }

            "...rollback" {
                val expectedException = RuntimeException()
                val recorder = spyk<SagaActionRollbackScope.(Int) -> Unit>()
                val nestedSaga = saga<Unit, Nothing> {
                    atomic({ 2 }, compensateWith = recorder)
                    atomic({ 3 }, compensateWith = { throw expectedException })
                    atomic({ 4 }, compensateWith = recorder)
                    result {}
                }
                val saga = saga<Unit, Unit> {
                    atomic({ 1 }, compensateWith = recorder)
                    atomic(nestedSaga)
                    atomic({ 5 }, compensateWith = recorder)
                    result { fail(Unit) }
                }

                shouldThrow<SagaException> {
                    saga.execute()
                }.cause shouldBe expectedException
                verify(exactly = 1) { recorder(any(), 1) }
                verify(exactly = 1) { recorder(any(), 2) }
                verify(exactly = 0) { recorder(any(), more(2)) }
            }
        }
    }

    "test" {
        saga<Unit, Nothing> {
            result {}
            result {}
        }.execute()
    }
})

class SagaSamples : FreeSpec({
    "Parallel ops" {
        val result = saga<String, Nothing> {
            val pool = Executors.newFixedThreadPool(2)
            val s1 = atomic({
                pool.submit<String> {
                    println("Step 1 started")
                    Thread.sleep(1000)
                    "Hello".also { println("Step 1 finished") }
                }
            }, compensateWith = {})
            val result = atomic({
                println("Step 2 started")
                val v = s1.value.get()
                "$v world".also { println("Step 2 finished") }
            }, compensateWith = {})
            result { result.value.also(::println) }
        }.execute()

        result.shouldBeInstanceOf<Saga.Result.Completed<String>>()
            .value shouldBe "Hello world"
    }
})
