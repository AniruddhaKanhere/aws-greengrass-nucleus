/*
 * JNI entry points for the coreMQTT channel handler adapter.
 * These are called from Java CoreMqttNative class.
 */
#include "coremqtt_channel_handler.h"

#include <aws/common/allocator.h>
#include <aws/common/common.h>
#include <aws/cal/cal.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/io.h>
#include <aws/io/socket.h>
#include <aws/io/tls_channel_handler.h>
#include <jni.h>
#include <string.h>

/* Initialize CRT when the library is loaded */
__attribute__((visibility("default")))
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    (void)vm;
    (void)reserved;
    struct aws_allocator *alloc = aws_default_allocator();
    aws_common_library_init(alloc);
    aws_io_library_init(alloc);
    return JNI_VERSION_1_6;
}

/* Helper: get C string from jstring (caller must free) */
static char *s_jstring_to_cstr(JNIEnv *env, jstring jstr, struct aws_allocator *alloc) {
    if (jstr == NULL) return NULL;
    const char *utf = (*env)->GetStringUTFChars(env, jstr, NULL);
    size_t len = strlen(utf);
    char *copy = aws_mem_calloc(alloc, 1, len + 1);
    memcpy(copy, utf, len + 1);
    (*env)->ReleaseStringUTFChars(env, jstr, utf);
    return copy;
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    create
 * Signature: (ILjava/lang/Object;)J
 *
 * Creates the channel handler. Does NOT connect yet.
 */
JNIEXPORT jlong JNICALL Java_com_aws_greengrass_mqttclient_CoreMqttNative_create(
    JNIEnv *env,
    jclass cls,
    jint keep_alive_sec,
    jobject java_callback) {

    (void)cls;
    struct aws_allocator *alloc = aws_default_allocator();

    JavaVM *jvm = NULL;
    (*env)->GetJavaVM(env, &jvm);

    jobject callback_ref = (*env)->NewGlobalRef(env, java_callback);

    struct coremqtt_channel_handler *handler = coremqtt_channel_handler_new(
        alloc, jvm, callback_ref, (uint16_t)keep_alive_sec);

    /* Create our own event loop group and bootstrap */
    handler->event_loop_group = aws_event_loop_group_new_default(alloc, 1, NULL);
    struct aws_host_resolver_default_options resolver_opts = {
        .el_group = handler->event_loop_group,
        .max_entries = 8,
    };
    handler->host_resolver = aws_host_resolver_new_default(alloc, &resolver_opts);
    struct aws_client_bootstrap_options bootstrap_opts = {
        .event_loop_group = handler->event_loop_group,
        .host_resolver = handler->host_resolver,
    };
    handler->bootstrap = aws_client_bootstrap_new(alloc, &bootstrap_opts);

    return (jlong)(uintptr_t)handler;
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    destroy
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_aws_greengrass_mqttclient_CoreMqttNative_destroy(
    JNIEnv *env,
    jclass cls,
    jlong handle) {

    (void)env;
    (void)cls;
    struct coremqtt_channel_handler *handler = (struct coremqtt_channel_handler *)(uintptr_t)handle;
    coremqtt_channel_handler_destroy(handler);
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    connect
 * Signature: (JLjava/lang/String;IJLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)V
 *
 * Initiates TCP+TLS connection via aws-c-io bootstrap, then sends MQTT CONNECT.
 */
JNIEXPORT void JNICALL
__attribute__((optimize("O0")))
Java_com_aws_greengrass_mqttclient_CoreMqttNative_connect(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jstring jendpoint,
    jint port,
    jlong bootstrap_handle,
    jstring jcert_path,
    jstring jkey_path,
    jstring jca_path,
    jstring jclient_id) {

    (void)cls;
    struct aws_allocator *alloc = aws_default_allocator();
    struct coremqtt_channel_handler *handler = (struct coremqtt_channel_handler *)(uintptr_t)handle;

    /* Ensure CRT is initialized (must happen before any aws-c-io call) */
    /* Use volatile to prevent compiler from optimizing these away */
    void (*volatile init_common)(struct aws_allocator *) = aws_common_library_init;
    void (*volatile init_io)(struct aws_allocator *) = aws_io_library_init;
    init_common(alloc);
    init_io(alloc);

    const char *endpoint = (*env)->GetStringUTFChars(env, jendpoint, NULL);
    const char *cert_path = (*env)->GetStringUTFChars(env, jcert_path, NULL);
    const char *key_path = (*env)->GetStringUTFChars(env, jkey_path, NULL);
    const char *ca_path = (*env)->GetStringUTFChars(env, jca_path, NULL);
    const char *client_id = (*env)->GetStringUTFChars(env, jclient_id, NULL);

    /* Store client ID in handler for use during MQTT_Connect */
    if (handler->client_id) {
        aws_mem_release(alloc, handler->client_id);
    }
    handler->client_id = aws_mem_calloc(alloc, 1, strlen(client_id) + 1);
    memcpy(handler->client_id, client_id, strlen(client_id) + 1);

    struct aws_client_bootstrap *bootstrap = handler->bootstrap;

    /* Create TLS context from cert/key/CA file paths */
    struct aws_tls_ctx_options tls_ctx_options;
    AWS_ZERO_STRUCT(tls_ctx_options);
    if (aws_tls_ctx_options_init_client_mtls_from_path(&tls_ctx_options, alloc, cert_path, key_path)) {
        fprintf(stderr, "[coremqtt_jni] ERROR: aws_tls_ctx_options_init_client_mtls_from_path failed: %d (%s)\n",
                aws_last_error(), aws_error_name(aws_last_error()));
        if (handler->java_callback) {
            (*env)->CallVoidMethod(env, handler->java_callback,
                                   handler->on_connection_failure_mid, (jint)aws_last_error());
        }
        goto cleanup;
    }
    fprintf(stderr, "[coremqtt_jni] TLS ctx options init OK (cert=%s, key=%s)\n", cert_path, key_path);
    if (aws_tls_ctx_options_override_default_trust_store_from_path(&tls_ctx_options, NULL, ca_path)) {
        fprintf(stderr, "[coremqtt_jni] ERROR: override_default_trust_store failed: %d (%s)\n",
                aws_last_error(), aws_error_name(aws_last_error()));
        aws_tls_ctx_options_clean_up(&tls_ctx_options);
        if (handler->java_callback) {
            (*env)->CallVoidMethod(env, handler->java_callback,
                                   handler->on_connection_failure_mid, (jint)aws_last_error());
        }
        goto cleanup;
    }
    fprintf(stderr, "[coremqtt_jni] Trust store override OK (ca=%s)\n", ca_path);

    struct aws_tls_ctx *tls_ctx = aws_tls_client_ctx_new(alloc, &tls_ctx_options);
    aws_tls_ctx_options_clean_up(&tls_ctx_options);

    if (tls_ctx == NULL) {
        fprintf(stderr, "[coremqtt_jni] ERROR: aws_tls_client_ctx_new failed: %d (%s)\n",
                aws_last_error(), aws_error_name(aws_last_error()));
        if (handler->java_callback) {
            (*env)->CallVoidMethod(env, handler->java_callback,
                                   handler->on_connection_failure_mid, (jint)aws_last_error());
        }
        goto cleanup;
    }
    fprintf(stderr, "[coremqtt_jni] TLS client ctx created OK\n");

    /* Set up TLS connection options */
    struct aws_tls_connection_options tls_conn_options;
    AWS_ZERO_STRUCT(tls_conn_options);
    aws_tls_connection_options_init_from_ctx(&tls_conn_options, tls_ctx);
    struct aws_byte_cursor host_cursor = aws_byte_cursor_from_c_str(endpoint);
    aws_tls_connection_options_set_server_name(&tls_conn_options, alloc, &host_cursor);

    /* Socket options */
    struct aws_socket_options socket_options = {
        .type = AWS_SOCKET_STREAM,
        .domain = AWS_SOCKET_IPV4,
        .connect_timeout_ms = 10000,
    };

    /* Initiate connection through aws-c-io bootstrap */
    struct aws_socket_channel_bootstrap_options channel_options = {
        .bootstrap = bootstrap,
        .host_name = endpoint,
        .port = (uint32_t)port,
        .socket_options = &socket_options,
        .tls_options = &tls_conn_options,
        .setup_callback = coremqtt_on_channel_setup,
        .shutdown_callback = coremqtt_on_channel_shutdown,
        .user_data = handler,
    };

    int result = aws_client_bootstrap_new_socket_channel(&channel_options);
    fprintf(stderr, "[coremqtt_jni] aws_client_bootstrap_new_socket_channel returned: %d (endpoint=%s:%d)\n",
            result, endpoint, (int)port);
    if (result != AWS_OP_SUCCESS) {
        fprintf(stderr, "[coremqtt_jni] ERROR: bootstrap_new_socket_channel failed: %d (%s)\n",
                aws_last_error(), aws_error_name(aws_last_error()));
        if (handler->java_callback) {
            (*env)->CallVoidMethod(env, handler->java_callback,
                                   handler->on_connection_failure_mid, (jint)aws_last_error());
        }
    }

    aws_tls_connection_options_clean_up(&tls_conn_options);
    aws_tls_ctx_release(tls_ctx);

cleanup:
    (*env)->ReleaseStringUTFChars(env, jendpoint, endpoint);
    (*env)->ReleaseStringUTFChars(env, jcert_path, cert_path);
    (*env)->ReleaseStringUTFChars(env, jkey_path, key_path);
    (*env)->ReleaseStringUTFChars(env, jca_path, ca_path);
    (*env)->ReleaseStringUTFChars(env, jclient_id, client_id);
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    disconnect
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_aws_greengrass_mqttclient_CoreMqttNative_disconnect(
    JNIEnv *env,
    jclass cls,
    jlong handle) {

    (void)env;
    (void)cls;
    struct coremqtt_channel_handler *handler = (struct coremqtt_channel_handler *)(uintptr_t)handle;

    if (handler->is_connected && handler->slot && handler->slot->channel) {
        MQTTSuccessFailReasonCode_t reason = 0; /* Normal disconnection */
        MQTT_Disconnect(&handler->mqtt_ctx, NULL, &reason);
        aws_channel_shutdown(handler->slot->channel, 0);
    }
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    publish
 * Signature: (JLjava/lang/String;[BIZLjava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_com_aws_greengrass_mqttclient_CoreMqttNative_publish(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jstring jtopic,
    jbyteArray jpayload,
    jint qos,
    jboolean retain,
    jobject jfuture) {

    (void)cls;
    struct coremqtt_channel_handler *handler = (struct coremqtt_channel_handler *)(uintptr_t)handle;

    const char *topic = (*env)->GetStringUTFChars(env, jtopic, NULL);
    jsize payload_len = jpayload ? (*env)->GetArrayLength(env, jpayload) : 0;
    jbyte *payload = payload_len > 0 ? (*env)->GetByteArrayElements(env, jpayload, NULL) : NULL;

    jobject future_ref = (*env)->NewGlobalRef(env, jfuture);

    coremqtt_publish(handler, topic, (const uint8_t *)payload, (size_t)payload_len,
                     (MQTTQoS_t)qos, (bool)retain, future_ref);

    if (payload) (*env)->ReleaseByteArrayElements(env, jpayload, payload, JNI_ABORT);
    (*env)->ReleaseStringUTFChars(env, jtopic, topic);
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    subscribe
 * Signature: (JLjava/lang/String;ILjava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_com_aws_greengrass_mqttclient_CoreMqttNative_subscribe(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jstring jtopic,
    jint qos,
    jobject jfuture) {

    (void)cls;
    struct coremqtt_channel_handler *handler = (struct coremqtt_channel_handler *)(uintptr_t)handle;

    const char *topic = (*env)->GetStringUTFChars(env, jtopic, NULL);
    jobject future_ref = (*env)->NewGlobalRef(env, jfuture);

    coremqtt_subscribe(handler, topic, (MQTTQoS_t)qos, future_ref);

    (*env)->ReleaseStringUTFChars(env, jtopic, topic);
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    unsubscribe
 * Signature: (JLjava/lang/String;Ljava/lang/Object;)V
 */
JNIEXPORT void JNICALL Java_com_aws_greengrass_mqttclient_CoreMqttNative_unsubscribe(
    JNIEnv *env,
    jclass cls,
    jlong handle,
    jstring jtopic,
    jobject jfuture) {

    (void)cls;
    struct coremqtt_channel_handler *handler = (struct coremqtt_channel_handler *)(uintptr_t)handle;

    const char *topic = (*env)->GetStringUTFChars(env, jtopic, NULL);
    jobject future_ref = (*env)->NewGlobalRef(env, jfuture);

    coremqtt_unsubscribe(handler, topic, future_ref);

    (*env)->ReleaseStringUTFChars(env, jtopic, topic);
}

/*
 * Class:     com_aws_greengrass_mqttclient_CoreMqttNative
 * Method:    isConnected
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_aws_greengrass_mqttclient_CoreMqttNative_isConnected(
    JNIEnv *env,
    jclass cls,
    jlong handle) {

    (void)env;
    (void)cls;
    struct coremqtt_channel_handler *handler = (struct coremqtt_channel_handler *)(uintptr_t)handle;
    return (jboolean)handler->is_connected;
}
