.PHONY: deploy
deploy: --check-env
	helm upgrade --install api-preprocessor . \
		--namespace=$(NAMESPACE) \
		--set image=$(IMAGE) \
		--set sourcePath=$(SOURCE_PATH) \
		--set sinkPath=$(SINK_PATH) \
		--set apiPreprocessorSubscriptionId=$(API_PREPROCESSOR_SUBSCRIPTION_ID)

.PHONY: --check-env
--check-env:
	@test $${NAMESPACE?environment variable is required}
	@test $${IMAGE?environment variable is required}
	@test $${SOURCE_PATH?environment variable is required}
	@test $${SINK_PATH?environment variable is required}
	@test $${API_PREPROCESSOR_SUBSCRIPTION_ID?environment variable is required}
