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

import com.google.common.base.Predicate;

import java.lang.reflect.Field;
import java.util.Set;

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
            Field field = clazz.getDeclaredField(fieldName);
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            field.set(instance, value);
            field.setAccessible(accessible);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(
                    "Failed to set field '" + fieldName + "' on '" + instance + "': " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(
                    "Failed to set field '" + fieldName + "' on '" + instance + "': " + e.getMessage(), e);
        }
    }

    public static Set<Field> getAllFields(Class aClass, Predicate<? super Field> predicate) {
        return org.reflections.ReflectionUtils.getAllFields(aClass, predicate);
    }

    public static <T> T getFieldValue(Object testCase, String name, Class<T> type) {
        try {
            Field field = testCase.getClass().getDeclaredField(name);
            boolean accessible = field.isAccessible();
            field.setAccessible(true);
            Object value = field.get(testCase);
            field.setAccessible(accessible);
            return (T) value;
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException(
                    "Failed to lookup field '" + name + "' for object '" + testCase + "': " + e.getMessage(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Failed to get value of field '" + name + "' for object '" + testCase + "': " + e.getMessage(), e);
        }
    }
}
