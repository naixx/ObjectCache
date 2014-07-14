package com.iainconnor.objectcache;

import com.google.gson.Gson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;

import rx.Observable;
import rx.Subscriber;

public class CacheManager {

    private final static int CACHE_RUSH_SECONDS = 60 * 2;
    private static CacheManager                  ourInstance;
    private        DiskCache                     diskCache;
    private        HashMap<String, CachedObject> runtimeCache;

    public static CacheManager getInstance(DiskCache diskCache) {
        if (ourInstance == null) {
            ourInstance = new CacheManager(diskCache);
        }

        return ourInstance;
    }

    private CacheManager(DiskCache diskCache) {
        this.diskCache = diskCache;
        runtimeCache = new HashMap<String, CachedObject>();
    }

    public boolean exists(String key) {
        boolean result = false;

        try {
            result = diskCache.contains(key);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    public Observable<Object> unset(String key) {
        return put(key, null, -1);
    }

    public Observable<Object> unsetAsync(String key) {
        return put(key, null, -1);
    }

    public Observable<Object> put(String key, Object object) {
        return put(key, object, -1);
    }

    public Observable<Object> put(final String key, final Object object, final int expiryTimeSeconds) {
        return Observable.create(new Observable.OnSubscribe<Object>() {
            @Override
            public void call(Subscriber<? super Object> subscriber) {
                try {
                    String payloadJson = new Gson().toJson(object);
                    CachedObject cachedObject = new CachedObject(payloadJson, expiryTimeSeconds);
                    String json = new Gson().toJson(cachedObject);
                    runtimeCache.put(key, cachedObject);
                    diskCache.setKeyValue(key, json);
                    subscriber.onNext(new Object());
                    subscriber.onCompleted();
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
        });
    }

    public void clear() throws IOException {
        runtimeCache.clear();
        diskCache.clearCache();
    }

    public enum ExpiryTimes {
        ONE_SECOND(1),
        ONE_MINUTE(60),
        ONE_HOUR(60 * 60),
        ONE_DAY(60 * 60 * 24),
        ONE_WEEK(60 * 60 * 24 * 7),
        ONE_MONTH(60 * 60 * 24 * 30),
        ONE_YEAR(60 * 60 * 24 * 365);

        private final int seconds;

        ExpiryTimes(int seconds) {
            this.seconds = seconds;
        }

        public int asSeconds() {
            return seconds;
        }
    }

    public <T> Observable<T> get(final String key, final Class objectClass, final Type objectType) {
        return Observable.create(new Observable.OnSubscribe<T>() {
            @Override
            public void call(Subscriber<? super T> subscriber) {
                try {
                    T result = null;

                    CachedObject runtimeCachedObject = runtimeCache.get(key);
                    if (runtimeCachedObject != null && !runtimeCachedObject.isExpired()) {
                        result = new Gson().fromJson(runtimeCachedObject.getPayload(), objectType);
                    } else {
                        String json = diskCache.getValue(key);
                        if (json != null) {
                            CachedObject cachedObject = new Gson().fromJson(json, CachedObject.class);
                            if (!cachedObject.isExpired()) {
                                runtimeCache.put(key, cachedObject);
                                result = new Gson().fromJson(cachedObject.getPayload(), objectType);
                            } else {
                                // To avoid cache rushing, we insert the value back in the cache with a longer expiry
                                // Presumably, whoever received this expiration result will have inserted a fresh value by now
                                put(key, new Gson().fromJson(cachedObject.getPayload(), objectType), CACHE_RUSH_SECONDS).subscribe(new Subscriber<Object>() {
                                    @Override
                                    public void onCompleted() { }

                                    @Override
                                    public void onError(Throwable e) { }

                                    @Override
                                    public void onNext(Object o) { }
                                });
                            }
                        }
                    }
                    if (result != null) {
                        subscriber.onNext(result);
                    }
                    subscriber.onCompleted();
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
        });
    }
}
