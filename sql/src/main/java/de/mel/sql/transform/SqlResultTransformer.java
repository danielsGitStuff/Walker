package de.mel.sql.transform;

import com.sun.org.apache.xpath.internal.operations.Bool;

public abstract class SqlResultTransformer {
    public abstract <T> T convert(Class<T> resultClass, Object value);

    public static SqlResultTransformer sqliteResultSetTransformer() {
        return new SqlResultTransformer() {
            @Override
            public <T> T convert(Class<T> resultClass, Object value) {
                if (value == null)
                    return null;
                if (String.class.isAssignableFrom(resultClass)) {
                    return (T) value;
                } else if (Number.class.isAssignableFrom(resultClass)) {
                    NumberTransformer nt = NumberTransformer.forType((Class<? extends Number>) resultClass);
                    // numbers may return als actual numbers
                    if (Number.class.isAssignableFrom(value.getClass())) {
                        return (T) nt.cast((Number) value);
                    }
                    // but sometimes 0 and 1 are represented as booleans
                    else if (Boolean.class.isAssignableFrom(value.getClass())) {
                        boolean b = (boolean) value;
                        if (b)
                            return (T) nt.cast(1);
                        return (T) nt.cast(0);
                    }
                }
//                else if (boolean.class.isAssignableFrom(resultClass)) {
//                    int i = (int) NumberTransformer.forType(int.class).cast((Number) value);
//                    System.out.println("SqlResultTransformer.convert");
//                    if ()
//                }
                else {
                    NumberTransformer nt = NumberTransformer.forType(int.class);
                    // but sometimes 0 and 1 are represented as booleans
                    if (Number.class.isAssignableFrom(value.getClass()) && (Boolean.class.isAssignableFrom(resultClass) || boolean.class.isAssignableFrom(resultClass))) {
                        Integer i = (Integer) nt.cast((Number) value);
                        if (i == 1)
                            return (T) new Boolean(true);
                        else if (i == 0)
                            return (T) new Boolean(false);
                    }
                    return (T) NumberTransformer.forType((Class<? extends Number>) resultClass).cast((Number) value);

//                    NumberTransformer nt = NumberTransformer.forType((Class<? extends Number>) resultClass);
//                    return (T) nt.cast((Number) value)
                }
                if (!resultClass.equals(value.getClass())) {
                    System.out.println("SqlResultTransformer.convert.error");
//                    NumberTransformer nt = NumberTransformer.forType((Class<? extends Number>) resultClass);
//                    T casted = (T) nt.cast((Number) value);
//                    return casted;
                }
                return (T) value;
            }
        };
    }
}
