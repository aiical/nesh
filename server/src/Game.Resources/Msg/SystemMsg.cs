namespace Game.Resources.Msg
{
    public class SystemMsg
    {
        public class CLIENT
        {
            public const int ACCESS_TOKEN              = 1;

            public const int CREATE_ROLE               = 2;

            public const int SELECT_ROLE               = 3;

            public const int ONLINE                    = 11;

            public const int CUSTOM                    = 100;
        }

        public class SERVER
        {
            public const int ACCESS_TOKEN              = 1;

            public const int CREATE_ROLE               = 2;

            public const int LOAD_ENTITIES             = 12;

            public const int SYNC_ENTITY               = 13;

            public const int SYNC_FIELD                = 14;

            public const int SYNC_TABLE                = 15;

            public const int CUSTOM                    = 100;
        }
    }
}
