package com.datastax.driver.core;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.RegularStatement.ValueDefinition;

public class ValueDefinitionAssert extends AbstractAssert<ValueDefinitionAssert, ValueDefinition> {

    ValueDefinitionAssert(ValueDefinition actual) {
        super(actual, ValueDefinitionAssert.class);
    }

    public ValueDefinitionAssert hasIndex(int expected) {
        assertThat(actual.getIndex()).isEqualTo(expected);
        assertThat(actual.getName()).isNull();
        return this;
    }

    public ValueDefinitionAssert hasName(String expected) {
        assertThat(actual.getName()).isEqualTo(expected);
        assertThat(actual.getIndex()).isEqualTo(-1);
        return this;
    }

    public ValueDefinitionAssert hasType(DataType expected) {
        assertThat(actual.getType()).isEqualTo(expected);
        return this;
    }
}
