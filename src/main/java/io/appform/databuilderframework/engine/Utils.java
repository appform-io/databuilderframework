package io.appform.databuilderframework.engine;

import com.google.common.base.CaseFormat;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.appform.databuilderframework.annotations.DataBuilderClassInfo;
import io.appform.databuilderframework.annotations.DataBuilderInfo;
import io.appform.databuilderframework.model.Data;
import io.appform.databuilderframework.model.DataBuilderMeta;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ConcurrentHashMap;

import java.util.*;

/**
 * Utilities for various stuff
 */
@UtilityClass
@Slf4j
public final class Utils {

    private static final ConcurrentHashMap<Class<?>, String> CLASS_TO_NAME_MAPPING = new ConcurrentHashMap<>();

    public static String name(Object object) {
        return name(object.getClass());
    }

    public static String name(Class<?> clazz) {
        return CLASS_TO_NAME_MAPPING.computeIfAbsent(clazz,
                aClass -> CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, clazz.getSimpleName()));
    }

    public static<T> boolean isEmpty(Collection<T> collection) {
        return null == collection || collection.isEmpty();
    }

    public static<K,V> boolean isEmpty(Map<K,V> collection) {
        return null == collection || collection.isEmpty();
    }

    public static<T> Set<T> sanitize(Set<T> collection) {
        return isEmpty(collection)
                ? Collections.emptySet()
                : collection;
    }

    public static<T> List<T> sanitize(List<T> collection) {
        return isEmpty(collection)
                ? Collections.emptyList()
                : collection;
    }

    public static<K,V> Map<K, V> sanitize(Map<K,V> collection) {
        return isEmpty(collection)
                ? Collections.emptyMap()
                : collection;
    }

    public static<T extends DataBuilder> DataBuilderMeta meta(T annotatedDataBuilder) {
        return meta(annotatedDataBuilder.getClass());
    }

    public static DataBuilderMeta meta(Class<? extends DataBuilder> annotatedDataBuilder) {
        DataBuilderInfo info = annotatedDataBuilder.getAnnotation(DataBuilderInfo.class);
        if(null != info) {
            return new DataBuilderMeta(
                    ImmutableSet.copyOf(info.consumes()),
                    info.produces(),
                    info.name(),
                    ImmutableSet.copyOf(info.optionals()),
                    ImmutableSet.copyOf(info.accesses()));
        }
        else {
            DataBuilderClassInfo dataBuilderClassInfo = annotatedDataBuilder.getAnnotation(DataBuilderClassInfo.class);
            if(null == dataBuilderClassInfo) {
                return null;
            }
            Set<String> consumes = Sets.newHashSet();
            Set<String> optionals = Sets.newHashSet();
            Set<String> access = Sets.newHashSet();
            //TODO (gokul) remove getCannonicalName() and inject a Handler for client to customize this

            for (Class<? extends Data> data : dataBuilderClassInfo.consumes()) {
                consumes.add(Utils.name(data));
            }

            for (Class<? extends Data> data : dataBuilderClassInfo.optionals()) {
                optionals.add(Utils.name(data));
            }

            for (Class<? extends Data> data : dataBuilderClassInfo.accesses()) {
                access.add(Utils.name(data));
            }

            final String name = dataBuilderClassInfo.name();
            return new DataBuilderMeta(
                    ImmutableSet.copyOf(consumes),
                    Utils.name(dataBuilderClassInfo.produces()),
                    Strings.isNullOrEmpty(name)
                        ? Utils.name(annotatedDataBuilder)
                        : name,
                    ImmutableSet.copyOf(optionals),
                    ImmutableSet.copyOf(access)
            );
        }
    }
}
