package com.madhouse.util;

/**
 * Created by WUJUNFENG on 2017/5/9.
 */
public final class KeyValuePair<T1, T2> {
    public final T1 first;
    public final T2 second;
    public KeyValuePair(T1 first, T2 second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int hashCode() {
        int result = 1;
        final int prime = 31;
        result = result * prime + ((this.first != null) ? this.first.hashCode() : 0);
        result = result * prime + ((this.second != null) ? this.second.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof KeyValuePair) {
            if (this != obj) {
                KeyValuePair<T1, T2> that = (KeyValuePair<T1, T2>)(obj);
                if (this.first == that.first && this.second == that.second) {
                    return true;
                }

                if (this.first.equals(that.first) && this.second.equals(that.second)) {
                    return true;
                }
            } else {
                return true;
            }
        }

        return false;
    }
}
