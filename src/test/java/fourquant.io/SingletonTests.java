package fourquant.io;

/**
 * A non-serializable class with Singleton members to test how code is run on the workers
 * Created by mader on 4/25/15.
 */
public class SingletonTests {
    protected static Long birthTime = null;
    public static synchronized long getBirthTime() {
        if(!isAlive()) {
            birthTime = new Long(System.nanoTime());
        }
        return birthTime;
    }

    public static boolean isAlive() {return !(birthTime==null);}

    public static long getAliveTime() {
        return System.nanoTime()-birthTime.longValue();
    }
}
