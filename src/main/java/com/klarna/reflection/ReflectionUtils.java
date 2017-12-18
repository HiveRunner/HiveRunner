/*
 * Copyright 2013 Klarna AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.klarna.reflection;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Collection of Reflection related helper functions.
 */
public final class ReflectionUtils {

    /**
     * Private constructor
     */
    private ReflectionUtils() {
    }

    public static void setStaticField(Class clazz, String fieldName, Object value) {
        setField(clazz, null, fieldName, value);
    }

    public static void setField(Object instance, String fieldName, Object value) {
        setField(instance.getClass(), instance, fieldName, value);
    }

    private static void setField(Class clazz, Object instance, String fieldName, Object value) {
        try {
            final Optional<Field> optional = getField(clazz, fieldName);
            Preconditions.checkArgument(optional.isPresent(), "Failed to set field '" + fieldName + "' on '" + instance);
            final Field field = optional.get();

            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            field.set(instance, value);
            field.setAccessible(accessible);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Failed to set field '" + fieldName + "' on '" + instance + "': " + e.getMessage(), e);
        }
    }

    /**
     * Finds the first Field with given field name in the Class and in its super classes.
     *
     * @param type      The Class type
     * @param fieldName The field name to get
     * @return an Optional<Field>. Use isPresent() to find out if the field name was found.
     */
    public static Optional<Field> getField(Class<?> type, final String fieldName) {
        Optional<Field> field = Iterables.tryFind(newArrayList(type.getDeclaredFields()), havingFieldName(fieldName));

        if (!field.isPresent() && type.getSuperclass() != null){
            field = getField(type.getSuperclass(), fieldName);
        }

        return field;
    }

    public static Set<Field> getAllFields(Class aClass, Predicate<? super Field> predicate) {
        return org.reflections.ReflectionUtils.getAllFields(aClass, predicate);
    }

    public static <T> T getFieldValue(Object testCase, String name, Class<T> type) {
        return getFieldValue(testCase, testCase.getClass(), name, type, false);
    }

    public static <T> T getStaticFieldValue(Class testCaseClass, String name, Class<T> type) {
        return getFieldValue(null, testCaseClass, name, type, true);
    }

    private static <T> T getFieldValue(Object testCase, Class testCaseClass, String name, Class<T> type, boolean isStatic) {
        try {
            Field field = testCaseClass.getDeclaredField(name);
            boolean accessible = field.isAccessible();

            Preconditions.checkState(field.getType().isAssignableFrom(type), "Field %s must be assignable from ", type);
            Preconditions.checkState(!isStatic || Modifier.isStatic(field.getModifiers()), "Field %s must be static ", field);

            field.setAccessible(true);
            Object value = field.get(testCase);
            field.setAccessible(accessible);
            return (T) value;
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(
                    "Failed to lookup field '" + name + "' for '" + testCaseClass + "': " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Failed to get value of field '" + name + "' for '" + testCaseClass + "': " + e.getMessage(), e);
        }
    }

    public static boolean isOfType(Field setupScriptField, Class type) {
        return setupScriptField.getType().isAssignableFrom(type);
    }


    private static Predicate<Field> havingFieldName(final String fieldName) {
        return new Predicate<Field>() {
            @Override
            public boolean apply(@Nullable Field field) {
                return fieldName.equals(field.getName());
            }
        };
    }

}
