using Microsoft.EntityFrameworkCore;

namespace TestApp
{
    public class MyContext : DbContext
    {
        public DbSet<Blog> Blogs { get; set; }
        public DbSet<Post> Posts { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNpgsql("User ID=postgres;Password=mysecretpassword;Host=localhost;Port=5432");
        }
    }
}