import rx


def split_payload():
    def _split(source):
        def subscribe(observer, scheduler):
            def on_next(obj):
                try:
                    obj = obj.value
                    observer.on_next(obj)
                except AttributeError as e:
                    observer.on_error(f"Kafka consumer record does not contain a value. Details: {e}")

            return source.subscribe(
                on_next,
                observer.on_error,
                observer.on_completed,
                scheduler
            )
        return rx.create(subscribe)
    return _split