package io.appform.databuilderframework.engine;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.appform.databuilderframework.model.DataBuilderMeta;
import lombok.val;

import java.util.*;

/**
 * Metadata manager class for {@link DataBuilder} implementations.
 * Data stored here is used by {@link ExecutionGraphGenerator} and
 * {@link DataFlowExecutor} classes for building and executing
 * data-flows respectively.
 */
public class DataBuilderMetadataManager {
    private final Map<String, Class<? extends DataBuilder>> dataBuilders;
    private final Map<String, DataBuilderMeta> meta;
    private final Map<String, List<DataBuilderMeta>> producedToProducerMap;
    private final Map<String, TreeSet<DataBuilderMeta>> consumesMeta;
    private final Map<String, TreeSet<DataBuilderMeta>> optionalsMeta;
    private final Map<String, TreeSet<DataBuilderMeta>> accessesMeta;

    private DataBuilderMetadataManager(
            Map<String, Class<? extends DataBuilder>> dataBuilders,
            Map<String, DataBuilderMeta> meta,
            Map<String, List<DataBuilderMeta>> producedToProducerMap,
            Map<String, TreeSet<DataBuilderMeta>> consumesMeta,
            Map<String, TreeSet<DataBuilderMeta>> optionalsMeta,
            Map<String, TreeSet<DataBuilderMeta>> accessesMeta) {
        this.dataBuilders = dataBuilders;
        this.meta = meta;
        this.producedToProducerMap = producedToProducerMap;
        this.consumesMeta = consumesMeta;
        this.optionalsMeta = optionalsMeta;
        this.accessesMeta = accessesMeta;
    }

    public DataBuilderMetadataManager() {
        this(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    public DataBuilderMetadataManager register(Class<? extends DataBuilder> annotatedDataBuilder) throws DataBuilderFrameworkException {
        val dataBuilderMeta = Utils.meta(annotatedDataBuilder);
        Objects.requireNonNull(dataBuilderMeta,
                                   "No useful annotations found on class. Use DataBuilderInfo or DataBuilderClassInfo to annotate");
        return register(dataBuilderMeta, annotatedDataBuilder);
    }

    /**
     * Register builder by using meta directly.
     *
     * @param dataBuilderMeta Meta about the builder
     * @param dataBuilder     The actual databuilder class
     * @return this
     * @throws DataBuilderFrameworkException
     */
    public DataBuilderMetadataManager register(
            DataBuilderMeta dataBuilderMeta,
            Class<? extends DataBuilder> dataBuilder) throws DataBuilderFrameworkException {
        return register(dataBuilderMeta.getConsumes(),
                        dataBuilderMeta.getOptionals(),
                        dataBuilderMeta.getAccess(),
                        dataBuilderMeta.getProduces(),
                        dataBuilderMeta.getName(),
                        dataBuilder);
    }

    /**
     * Register metadata for a {@link DataBuilder} implementation.
     *
     * @param consumes    List of {@link io.appform.databuilderframework.model.Data} this builder consumes
     * @param produces    {@link io.appform.databuilderframework.model.Data} produced by this builder
     * @param builder     Name for this builder. there is no namespacing. Name needs to be unique
     * @param dataBuilder The class of the builder to be created
     * @throws DataBuilderFrameworkException In case of name conflict
     */
    public DataBuilderMetadataManager register(
            Set<String> consumes, String produces,
            String builder, Class<? extends DataBuilder> dataBuilder) throws DataBuilderFrameworkException {
        return register(consumes, null, null, produces, builder, dataBuilder);
    }

    public DataBuilderMetadataManager registerWithOptionals(
            Set<String> consumes, Set<String> optionals, String produces,
            String builder, Class<? extends DataBuilder> dataBuilder) throws DataBuilderFrameworkException {
        return register(consumes, optionals, null, produces, builder, dataBuilder);
    }

    public DataBuilderMetadataManager registerWithAccess(
            Set<String> consumes, Set<String> access, String produces,
            String builder, Class<? extends DataBuilder> dataBuilder) throws DataBuilderFrameworkException {
        return register(consumes, null, access, produces, builder, dataBuilder);
    }

    public DataBuilderMetadataManager register(
            Set<String> consumes, Set<String> optionals, Set<String> access, String produces,
            String builder, Class<? extends DataBuilder> dataBuilder) throws DataBuilderFrameworkException {
        if (meta.containsKey(builder)) {
            throw new DataBuilderFrameworkException(DataBuilderFrameworkException.ErrorCode.BUILDER_EXISTS,
                                                    "A builder with name " + builder + " already exists");
        }
        val metadata = new DataBuilderMeta(consumes, produces, builder, optionals, access);
        meta.put(builder, metadata);
        producedToProducerMap
                .computeIfAbsent(produces, k -> new ArrayList<>())
                .add(metadata);
        consumes.forEach(
                consumesData -> consumesMeta.computeIfAbsent(consumesData, k -> new TreeSet<>()).add(metadata));

        if (optionals != null) {
            optionals.forEach(
                    optionalsData -> optionalsMeta.computeIfAbsent(optionalsData, k -> new TreeSet<>()).add(metadata));
        }
        if (access != null) {
            access.forEach(
                    accessData -> accessesMeta.computeIfAbsent(accessData, k -> new TreeSet<>()).add(metadata));
        }
        dataBuilders.put(builder, dataBuilder);
        return this;
    }

    /**
     * Get {@link io.appform.databuilderframework.model.DataBuilderMeta} meta for all builders that consume this data.
     *
     * @param data Name of the data to be consumed
     * @return Set of {@link io.appform.databuilderframework.model.DataBuilderMeta} for matching builders
     * or null if none found
     */
    public Set<DataBuilderMeta> getConsumesSetFor(final String data) {
        return consumesMeta.get(data);
    }

    /**
     * Get {@link io.appform.databuilderframework.model.DataBuilderMeta} for builder that produces this data
     *
     * @param data Data being produced
     * @return List of {@link io.appform.databuilderframework.model.DataBuilderMeta} that are capable of
     * producing this data or null if not found.
     */
    public List<DataBuilderMeta> getMetaForProducerOf(final String data) {
        return producedToProducerMap.get(data);
    }

    /**
     * Get {@link io.appform.databuilderframework.model.DataBuilderMeta} for a particular builder.
     *
     * @param builderName Name of the builder
     * @return Meta if found, null otherwise
     */
    public DataBuilderMeta get(String builderName) {
        return meta.get(builderName);
    }

    /**
     * Get all {@link io.appform.databuilderframework.model.DataBuilderMeta} for the for the provided names.
     *
     * @param builderNames List of data builder names
     * @return List of builder metadata
     */
    public Collection<DataBuilderMeta> get(List<String> builderNames) {
        return Maps.filterKeys(meta, Predicates.in(builderNames)).values();
    }

    public boolean contains(List<String> builderNames) {
        return meta.keySet().containsAll(builderNames);
    }

    /**
     * Get a derived class of {@link DataBuilder} for the given name.
     *
     * @param builderName Name of the builder
     * @return Class if found, null otherwise
     */
    public Class<? extends DataBuilder> getDataBuilderClass(String builderName) {
        return dataBuilders.get(builderName);
    }

    public DataBuilderMetadataManager immutableCopy() {
        return new DataBuilderMetadataManager(ImmutableMap.copyOf(dataBuilders),

                                              ImmutableMap.copyOf(meta),
                                              ImmutableMap.copyOf(producedToProducerMap),
                                              ImmutableMap.copyOf(consumesMeta),
                                              ImmutableMap.copyOf(optionalsMeta),
                                              ImmutableMap.copyOf(accessesMeta));
    }
}