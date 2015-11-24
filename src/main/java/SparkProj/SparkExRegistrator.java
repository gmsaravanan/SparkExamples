package SparkProj;

/**
 * Created by mohasa02 on 11/12/2015.
 */

import com.esotericsoftware.kryo.*;
import org.apache.spark.serializer.*;
import javax.annotation.*;
import java.io.*;
import java.lang.reflect.*;

public class SparkExRegistrator implements KryoRegistrator, Serializable{

    /**
     * register a class indicated by name
     *
     * @param kryo
     * @param s       name of a class - might not exist
     * @param handled Set of classes already handles
     */
    protected void doRegistration(@Nonnull Kryo kryo, @Nonnull String s ) {
        Class c;
        try {
            c = Class.forName(s);
            doRegistration(kryo,  c);
        }
        catch (ClassNotFoundException e) {
            return;
        }
    }

    /**
     * register a class
     *
     * @param kryo
     * @param s       name of a class - might not exist
     * @param handled Set of classes already handles
     */
    protected void doRegistration(final Kryo kryo , final Class pC) {
        if (kryo != null) {
            kryo.register(pC);
            // also register arrays of that class
            Class arrayType = Array.newInstance(pC, 0).getClass();
            kryo.register(arrayType);
        }
    }


    /**
     * do the real work of registering all classes
     * @param kryo
     */
    @Override
    public void registerClasses(@Nonnull Kryo kryo) {
        kryo.register(Object[].class);
        kryo.register(scala.Tuple2[].class);

        doRegistration(kryo, "scala.collection.mutable.WrappedArray$ofRef");
        doRegistration(kryo, "com.nielsen.perfengg.SparkD");

// and many more similar nines

    }


}
