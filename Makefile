include rust.mk

# Crates
$(eval $(call RUST_CRATE,LIB,src/redis/lib.rs))
$(eval $(call RUST_CRATE,TEST,src/redis/test.rs,--test))
$(eval $(call RUST_CRATE,EXAMPLE,src/example/main.rs))

# Crate dependencies
$(TEST_OUT): $(LIB_OUT)

# Convenience functions
all: lib
	
lib: $(LIB_OUT)

example: $(EXAMPLE_OUT)
	@./$(EXAMPLE_OUT)

test: $(TEST_OUT)
	@RUST_TEST_TASKS=1 ./$(TEST_OUT)

clean: $(LIB_CLEAN) $(EXAMPLE_CLEAN) $(TEST_CLEAN)

.PHONY: all lib example test clean
