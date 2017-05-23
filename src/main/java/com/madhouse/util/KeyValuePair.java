package com.madhouse.util;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public final class KeyValuePair<T1, T2> {
    public final T1 first;
    public final T2 sencond;
    public KeyValuePair(T1 first, T2 sencond) {
        this.first = first;
        this.sencond = sencond;
    }

    @Override
    public int hashCode() {
        int result = 1;
        final int prime = 31;
        result = result * prime + ((this.first != null) ? this.first.hashCode() : 0);
        result = result * prime + ((this.sencond != null) ? this.sencond.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof KeyValuePair) {
            if (this != obj) {
                KeyValuePair<T1, T2> that = (KeyValuePair<T1, T2>)(obj);
                if (this.first == that.first && this.sencond == that.sencond) {
                    return true;
                }

                if (this.first.equals(that.first) && this.sencond.equals(that.sencond)) {
                    return true;
                }
            } else {
                return true;
            }
        }

        return false;
    }
}
