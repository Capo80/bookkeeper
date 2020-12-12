package org.apache.bookkeeper.mytests;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utils {

    public static List<List<Object>> cartesianProduct(List<?>... sets) {
        if (sets.length < 2)
            throw new IllegalArgumentException(
                    "Can't have a product of fewer than two sets (got " +
                            sets.length + ")");

        return _cartesianProduct(0, sets);
    }

    private static List<List<Object>> _cartesianProduct(int index, List<?>... sets) {
        List<List<Object>> ret = new ArrayList<>();
        if (index == sets.length) {
            ret.add(new ArrayList<>());
        } else {
            for (Object obj : sets[index]) {
                for (List<Object> set : _cartesianProduct(index+1, sets)) {
                    set.add(obj);
                    ret.add(set);
                }
            }
        }
        return ret;
    }

    public static ByteBuf buildEntry(long ledgerId, long entryId) {
        final ByteBuf data = Unpooled.buffer();
        data.writeLong(ledgerId);
        data.writeLong(entryId);
        //data.writeLong(lastAddConfirmed);
        return data;
    }
    public static ByteBuf buildEntry(long ledgerId, long entryId, byte[] dataToEnter) {
        final ByteBuf data = Unpooled.buffer();
        data.writeLong(ledgerId);
        data.writeLong(entryId);
        data.writeBytes(dataToEnter);
        return data;
    }

}
